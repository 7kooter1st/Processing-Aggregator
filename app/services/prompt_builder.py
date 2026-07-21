import json
import logging
import re
from typing import Any

from app.models.schemas import ChunkContent, ContentType

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """OUTPUT RULES (HIGHEST PRIORITY):
1. Reply with ONE JSON object only. No markdown. No ```json. No text before or after JSON.
2. Do not write explanations, greetings, or comments.
3. Every file1_line and file2_line MUST be at most 200 characters.
4. If a source line is longer than 200 characters, return only a short excerpt around the difference (max 200 chars).
5. EVERY difference MUST include valid non-empty file1_span AND file2_span.
6. If you cannot point to the exact differing characters with spans — DO NOT add that item to differences.

JSON SCHEMA (exact keys, no extras):
{
  "identical": false,
  "differences": [
    {
      "line_number": 1,
      "file1_line": "фрагмент из документа 1 (макс 200 символов)",
      "file2_line": "фрагмент из документа 2 (макс 200 символов)",
      "file1_span": [12, 28],
      "file2_span": [12, 30]
    }
  ]
}

FIELD RULES:
- identical (boolean, required): true only if there are NO substantive differences; then differences MUST be [].
- differences (array, required): only substantive differences with valid spans.
- line_number (integer|null): 1-based line number.
- file1_line / file2_line (string|null): excerpt, length <= 200.
- file1_span / file2_span (array of exactly 2 integers, REQUIRED for each difference):
  - format [start, end]
  - 0-based, start inclusive, end exclusive
  - start < end
  - both indices must be inside the corresponding *_line string
  - must highlight the real differing substring (not the whole line unless the whole line differs)
  - example: "сумма 100 руб" vs "сумма 120 руб" → file1_span=[6,9], file2_span=[6,9]

IGNORE (do NOT report):
1. Tabs / spaces / repeated whitespace
2. Punctuation only (.,;:!?«»\"'()[]{}…—–-/, commas, double commas)
3. Letter case only (Траншей vs траншей)

If after ignoring 1–3 the texts match — identical=true, differences=[].

REPORT ONLY: different words, numbers, dates, amounts, names, missing/extra meaningful text.

TASK:
1. Two fragments (txt and/or image). Image → OCR first. Txt → use as-is.
2. Compare line by line after IGNORE rules.
3. Emit a difference only if you can fill BOTH spans correctly.
4. No substantive diffs → {"identical": true, "differences": []}

FINAL CHECK:
- Valid JSON only
- Lines <= 200 chars
- Every difference has file1_span and file2_span with start < end
- No case/punctuation/whitespace-only diffs
- No differences with empty/null/missing spans"""

USER_PROMPT_TEMPLATE = """Фрагмент {chunk_index} из {total_chunks}.

Сравни документы. Игнорируй только: пробелы/табы, пунктуацию, регистр.
Каждое отличие ОБЯЗАТЕЛЬНО с file1_span и file2_span [start, end].
Без валидных span — не включай отличие. Верни только JSON.

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


def _is_valid_span(span: Any, line: Any) -> bool:
    if not isinstance(line, str) or not line:
        return False
    if not isinstance(span, list) or len(span) != 2:
        return False
    start, end = span
    if not isinstance(start, int) or not isinstance(end, int):
        return False
    if start < 0 or end < 0 or start >= end:
        return False
    if end > len(line):
        return False
    return True


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
                span_key = "file1_span" if key == "file1_line" else "file2_span"
                span = item.get(span_key)
                if (
                    isinstance(span, list)
                    and len(span) == 2
                    and isinstance(span[0], int)
                    and isinstance(span[1], int)
                ):
                    item[span_key] = [
                        max(0, min(span[0], max_len)),
                        max(0, min(span[1], max_len)),
                    ]

    return fragment


def _filter_invalid_differences(fragment: dict[str, Any]) -> dict[str, Any]:
    """Drop differences without valid highlight spans before sending downstream."""
    differences = fragment.get("differences")
    if not isinstance(differences, list):
        return fragment

    kept: list[dict[str, Any]] = []
    dropped = 0

    for item in differences:
        if not isinstance(item, dict):
            dropped += 1
            continue

        file1_line = item.get("file1_line")
        file2_line = item.get("file2_line")
        file1_span = item.get("file1_span")
        file2_span = item.get("file2_span")

        if not _is_valid_span(file1_span, file1_line):
            dropped += 1
            logger.info(
                "[FILTER] drop diff without valid file1_span: line=%s span=%s",
                item.get("line_number"),
                file1_span,
            )
            continue

        if not _is_valid_span(file2_span, file2_line):
            dropped += 1
            logger.info(
                "[FILTER] drop diff without valid file2_span: line=%s span=%s",
                item.get("line_number"),
                file2_span,
            )
            continue

        # Both sides empty after ignore-style equality → false positive
        if file1_line == file2_line:
            dropped += 1
            logger.info(
                "[FILTER] drop identical lines marked as diff: line=%s",
                item.get("line_number"),
            )
            continue

        kept.append(item)

    if dropped:
        logger.info("[FILTER] removed %s invalid differences, kept %s", dropped, len(kept))

    fragment["differences"] = kept
    fragment["identical"] = len(kept) == 0
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
            return _filter_invalid_differences(_clamp_line_fields(parsed))
    except json.JSONDecodeError:
        logger.warning("Failed to parse Ollama JSON response")

    return None
