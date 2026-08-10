import json
import logging
import re
from typing import Any

from app.models.schemas import ChunkContent, ContentType

logger = logging.getLogger(__name__)

SYSTEM_PROMPT_OCR = """You are an OCR engine for scanned legal documents (Russian).
Task: extract text from the provided page image ONLY.

Rules:
1. Output plain text only. No explanations, greetings, comments, or JSON.
2. Keep reading order: top to bottom, left to right.
3. Tables: Markdown pipe tables (| col1 | col2 |) or stable "cell | cell" rows.
4. Preserve numbers, dates, names, amounts exactly as seen. Do not fix grammar or legal wording.
5. Stamps/signatures: use [печать] / [подпись]. Do not invent stamp text.
6. Unreadable fragments: [неразборчиво]. Do not guess missing words.
7. Do not compare documents. Do not evaluate errors."""

SYSTEM_PROMPT_COMPARE = """OUTPUT RULES (HIGHEST PRIORITY):
1. Reply with ONE valid JSON object only. No markdown. No ```json. No text before or after JSON.
2. Do not write explanations, greetings, comments, or any extra keys.
3. Every file1_line and file2_line MUST be at most 200 characters.
4. If an input line is longer than 200 characters, return only a short excerpt containing the differing part and minimal context (5-7 words around the difference).
5. Do not calculate line numbers, character positions, or highlight spans.

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
- identical (boolean, required): true ONLY if texts are identical after normalization described in IGNORE. If ANY word, number, abbreviation, or prefix differs — set identical=false.
- differences (array, required): every difference found. Empty array [] only when identical=true.
- file1_line / file2_line (string|null): exact excerpt, length <= 200, or null if line is absent.

IGNORE (do NOT report as differences):
1. Tabs, spaces, line breaks, and repeated whitespace (normalize all whitespace to single spaces before comparing).
2. Punctuation marks ONLY: . , ; : ! ? « » " ' ( ) [ ] { } … — – - /

DO NOT IGNORE — REPORT EVERY difference:
1. ANY changed, missing, or extra word, including:
   - Added or removed prefix "не" (e.g., "оплачено" vs "не оплачено", "допустимо" vs "недопустимо").
   - Added or removed prefix "без" (e.g., "оснований" vs "безосновательно").
   - Any suffix or infix change.
2. ANY changed, missing, or extra abbreviation or acronym (e.g., "ООО" vs "ОАО", "ФЗ" vs "УК", "№" vs "N").
3. ANY changed, missing, or extra number, digit, or numeral, including:
   - Different digits (e.g., "2024" vs "2025").
   - Different number formatting (e.g., "1 000" vs "1000" vs "1,000").
   - Different numeral forms (e.g., "5" vs "пять").
4. ANY changed, missing, or extra date component (day, month, year, separator).
5. ANY changed, missing, or extra name, surname, patronymic, company name, or address.
6. ANY changed, missing, or extra monetary amount or currency.
7. ANY reordered words or lines that change meaning.
8. ANY OCR artifacts: Latin letter instead of Cyrillic (a→а, o→о, e→е, c→с, p→р, x→х, y→у), digit 0 instead of letter O, digit 1 instead of letter l or I, and vice versa.
9. ANY case difference combined with other changes (e.g., "Иванов" vs "иванов" is a difference because word form changes, even though case-only would be ignored).

COMPARISON METHOD (mandatory):
1. Normalize whitespace: replace all tabs, newlines, and multiple spaces with single spaces; trim ends.
2. Strip ONLY the punctuation marks listed in IGNORE from both texts.
3. Split both normalized texts into tokens (words, numbers, abbreviations) by spaces.
4. Compare token by token in order.
5. If token counts differ, or any token at the same position differs — report it.
6. Do NOT paraphrase, do NOT interpret intent, do NOT "smooth over" near-matches. If tokens are not exactly equal after stripping ignored punctuation — report as difference.

TASK:
1. Compare two text fragments using the COMPARISON METHOD above.
2. Identify every difference listed in "DO NOT IGNORE".
3. Emit each difference as a pair of excerpts.
4. If no differences found after normalization, return {"identical": true, "differences": []}.

FINAL CHECK:
- Valid JSON only
- No extra text
- Lines <= 200 chars
- Every non-ignored difference is included
- identical=true ONLY when texts are truly identical after normalization
"""

OCR_USER_PROMPT_TEMPLATE = """Распознай текст страницы документа «{filename}».
Верни только текст."""

COMPARE_USER_PROMPT_TEMPLATE = """Фрагмент {chunk_index} из {total_chunks}.

Документ 1 ({file1_name}, тип: txt):
{file1_content}

Документ 2 ({file2_name}, тип: txt):
{file2_content}"""


def build_ocr_messages(chunk: ChunkContent) -> list[dict[str, Any]]:
    """Build Ollama messages for OCR of a single image chunk."""
    if chunk.content_type != ContentType.IMAGE:
        raise ValueError(
            f"OCR requires content_type=image, got {chunk.content_type!r} "
            f"for file {chunk.filename!r}"
        )

    user_message: dict[str, Any] = {
        "role": "user",
        "content": OCR_USER_PROMPT_TEMPLATE.format(filename=chunk.filename),
        "images": [chunk.content],
    }
    return [
        {"role": "system", "content": SYSTEM_PROMPT_OCR},
        user_message,
    ]


def build_compare_messages(
    chunk_index: int,
    total_chunks: int,
    file1: ChunkContent | None,
    file2: ChunkContent | None,
) -> list[dict[str, Any]]:
    """Build Ollama messages for text↔text comparison (no images)."""
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

    user_text = COMPARE_USER_PROMPT_TEMPLATE.format(
        chunk_index=chunk_index,
        total_chunks=total_chunks,
        file1_name=file1.filename,
        file1_content=file1.content,
        file2_name=file2.filename,
        file2_content=file2.content,
    )

    return [
        {"role": "system", "content": SYSTEM_PROMPT_COMPARE},
        {"role": "user", "content": user_text},
    ]


def extract_ocr_text(ollama_response: dict[str, Any]) -> str:
    """Extract plain OCR text; strip markdown fences if the model wrapped the output.

    Gemma4 may put tokens only in ``message.thinking`` when think mode is on;
    that is not usable OCR text — callers should send ``think: false`` and retry.
    """
    message = ollama_response.get("message") or {}
    content = message.get("content", "")
    if not isinstance(content, str):
        content = ""

    text = content.strip()
    if not text:
        thinking = message.get("thinking") or ""
        if isinstance(thinking, str) and thinking.strip():
            logger.warning(
                "OCR response has empty content but non-empty thinking "
                "(done_reason=%s, thinking_len=%s) — treat as empty OCR",
                ollama_response.get("done_reason"),
                len(thinking),
            )
        return ""

    if text.startswith("```"):
        text = re.sub(r"^```(?:\w+)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()

    return text


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
