import json
import logging
import re
from typing import Any

from app.models.schemas import ChunkContent, ContentType

logger = logging.getLogger(__name__)

CLASSIFICATION_PROMPT_VERSION = "hybrid-classifier-v1"

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

OCR_USER_PROMPT_TEMPLATE = """Распознай текст страницы документа «{filename}».
Верни только текст."""

SYSTEM_PROMPT_CLASSIFY = """Ты классифицируешь уже найденные алгоритмом
различия между двумя версиями юридического документа. Не ищи новые различия,
не объединяй разные candidate_id и не исправляй исходный текст.

Для каждого входного candidate_id верни ровно одно решение:
- substantive: после удаления только технического оформления остаётся замена,
  добавление или удаление содержимого;
- technical: различается только Markdown, нумерация/маркер в начале пункта,
  номер страницы, колонтитул, перенос или эквивалентный вид тире;
- alignment_error: абзацы относятся к разным местам или границы абзаца
  сопоставлены неправильно;
- ocr_uncertain: фрагменты сопоставлены, но отличие похоже на ошибку OCR и по
  одному тексту нельзя подтвердить напечатанный вариант.

Правила безопасности:
1. Не называй technical исчезновение/добавление «не» или «без».
2. Не называй technical замену слова, имени, адреса, аббревиатуры, суммы, даты
   или числа внутри предложения.
3. Номер в начале пункта может быть technical; число после слов «№»,
   «сумма», «дата», «статья», «Приложение» является содержимым.
4. Учитывай source_type и was_ocr, но не объявляй OCR-ошибку доказанной без
   изображения.
5. left_change/right_change должны дословно совпадать с полями changed
   соответствующего кандидата либо быть null.

Ответь только одним JSON-объектом:
{
  "decisions": [
    {
      "candidate_id": "точный входной ID",
      "category": "substantive|technical|alignment_error|ocr_uncertain",
      "technical_type": "markdown|numbering|page_number|list_marker|header_footer|dash|line_wrap|null",
      "left_change": "точный изменённый фрагмент или null",
      "right_change": "точный изменённый фрагмент или null",
      "reason": "краткая причина",
      "confidence": 0.0
    }
  ]
}
Не добавляй другие ключи. Верни все входные ID ровно по одному разу."""

CLASSIFY_USER_PROMPT_TEMPLATE = """Документ 1: {file1_name}
Документ 2: {file2_name}
Повторная проверка выравнивания: {alignment_retry}

Кандидаты из PostgreSQL и детерминированного diff (JSON):
{candidates_json}"""


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


def build_classification_messages(
    *,
    file1_name: str,
    file2_name: str,
    candidates: list[dict[str, Any]],
    alignment_retry: bool = False,
) -> list[dict[str, Any]]:
    """Build a candidate-ID anchored classification request."""
    user_text = CLASSIFY_USER_PROMPT_TEMPLATE.format(
        file1_name=file1_name,
        file2_name=file2_name,
        alignment_retry="да" if alignment_retry else "нет",
        candidates_json=json.dumps(
            candidates,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    )
    return [
        {"role": "system", "content": SYSTEM_PROMPT_CLASSIFY},
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


def extract_classification_decisions(
    ollama_response: dict[str, Any],
) -> list[dict[str, Any]] | None:
    """Extract the raw decision list; semantic validation happens per batch."""
    message = ollama_response.get("message") or {}
    content = message.get("content", "")
    if not isinstance(content, str) or not content.strip():
        return None

    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content)

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        logger.warning("Failed to parse Ollama classification JSON")
        return None

    decisions = parsed.get("decisions") if isinstance(parsed, dict) else None
    return decisions if isinstance(decisions, list) else None
