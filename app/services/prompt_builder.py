import json
import logging
import re
from typing import Any

from app.models.schemas import ChunkContent, ContentType

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """OUTPUT RULES (HIGHEST PRIORITY):
1. Reply with ONE valid JSON object only. No markdown. No ```json. No text before or after JSON.
2. Do not write explanations, greetings, comments, or any extra keys.
3. Every file1_line and file2_line MUST be at most 200 characters.
4. If an input line is longer than 200 characters, return only a short excerpt containing the differing part and minimal context.
5. Report every substantive difference.
6. Do not calculate line numbers, character positions, or highlight spans.

JSON SCHEMA (exact keys, no extras):
{
  "identical": false,
  "differences": [
    {
      "file1_line": "excerpt from document 1 (max 200 chars)",
      "file2_line": "excerpt from document 2 (max 200 chars)"
    }
  ]
}

FIELD RULES:
- identical (boolean, required): true only if there are NO substantive differences; then differences MUST be [].
- differences (array, required): all substantive differences.
- file1_line / file2_line (string|null): excerpt, length <= 200, or null if the line is missing.

IGNORE (do NOT report):
1. Tabs / spaces / repeated whitespace.
2. Punctuation only differences (.,;:!?«»"\'()[]{}…—–-/, commas, double commas).
3. Letter case only differences.

If texts match after applying IGNORE rules, set identical=true and differences=[].

REPORT ONLY substantive differences:
- different words
- numbers
- dates
- amounts
- names
- missing or extra meaningful text
- reordered meaningful fragments if the meaning changes

TASK:
1. Compare two fragments (txt and/or image). If image is provided, perform OCR first. If txt is provided, use it as-is.
2. Compare their contents after applying IGNORE rules.
3. Emit every substantive difference.
4. If there are no substantive differences, return {"identical": true, "differences": []}.

FINAL CHECK:
- Valid JSON only
- No extra text
- Lines <= 200 chars
- Every substantive difference is included
- No line numbers or highlight spans
- No case/punctuation/whitespace-only diffs"""

USER_PROMPT_TEMPLATE = """Фрагмент {chunk_index} из {total_chunks}.

Документ 1 ({file1_name}, тип: {file1_type}):
{file1_content}

Документ 2 ({file2_name}, тип: {file2_type}):
{file2_content}"""


def _format_chunk_content(chunk: ChunkContent) -> tuple[str, str, list[str]]:
    """Return content type label, text for prompt, and optional images for Ollama."""
    if chunk.content_type == ContentType.TEXT:
        return "txt", chunk.content, []

    return (
        "image",
        f"(изображение «{chunk.filename}» — распознай текст и используй для сравнения)",
        [chunk.content],
    )


def build_ollama_messages(
    chunk_index: int,
    total_chunks: int,
    file1: ChunkContent | None,
    file2: ChunkContent | None,
) -> list[dict[str, Any]]:
    file1 = file1 or ChunkContent(
        filename="unknown",
        format="unknown",
        content_type=ContentType.TEXT,
        content="(пусто)",
    )
    file2 = file2 or ChunkContent(
        filename="unknown",
        format="unknown",
        content_type=ContentType.TEXT,
        content="(пусто)",
    )

    file1_type, file1_text, file1_images = _format_chunk_content(file1)
    file2_type, file2_text, file2_images = _format_chunk_content(file2)

    user_text = USER_PROMPT_TEMPLATE.format(
        chunk_index=chunk_index,
        total_chunks=total_chunks,
        file1_name=file1.filename,
        file1_type=file1_type,
        file1_content=file1_text,
        file2_name=file2.filename,
        file2_type=file2_type,
        file2_content=file2_text,
    )

    images = file1_images + file2_images
    user_message: dict[str, Any] = {"role": "user", "content": user_text}
    if images:
        user_message["images"] = images

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        user_message,
    ]


def _clamp_line_fields(fragment: dict[str, Any], max_len: int = 200) -> dict[str, Any]:
    """Soft-enforce max length on line fields if the model ignores the limit."""
    differences = fragment.get("differences")
    if not isinstance(differences, list):
        return fragment

    for item in differences:
        if not isinstance(item, dict):
            continue
        for key in ("file1_line", "file2_line"):
            value = item.get(key)
            if isinstance(value, str) and len(value) > max_len:
                item[key] = value[:max_len]

    return fragment


def _clear_location_fields(fragment: dict[str, Any]) -> dict[str, Any]:
    """Temporarily disable line numbers and character highlighting."""
    differences = fragment.get("differences")
    if not isinstance(differences, list):
        return fragment

    for item in differences:
        if not isinstance(item, dict):
            continue
        item["line_number"] = None
        item["file1_span"] = None
        item["file2_span"] = None

    return fragment


def extract_comparison_fragment(ollama_response: dict[str, Any]) -> dict[str, Any] | None:
    """Extract comparison JSON from Ollama chat response."""
    message = ollama_response.get("message") or {}
    content = message.get("content", "")
    if not content:
        return None

    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content)

    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return _clear_location_fields(_clamp_line_fields(parsed))
    except json.JSONDecodeError:
        logger.warning("Failed to parse Ollama JSON response")

    return None
