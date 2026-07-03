import json
import logging
import re
from typing import Any

from app.models.schemas import ChunkContent, ContentType

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Ты отвечаешь только на строго поставленную задачу, без дополнительных слов и пояснений — строго в формате JSON следующего вида:

{
  "identical": false,
  "differences": [
    {
      "line_number": 1,
      "file1_line": "строка из первого документа",
      "file2_line": "строка из второго документа"
    }
  ]
}

Поля:
- "identical" (boolean, обязательно) — true, если все строки совпадают; false, если есть хотя бы одно отличие.
- "differences" (массив, обязательно) — список построчных расхождений. Если тексты идентичны — пустой массив [].
- "line_number" (integer или null) — номер строки, где найдено отличие (нумерация с 1).
- "file1_line" (string или null) — строка из первого документа.
- "file2_line" (string или null) — строка из второго документа.

Если строки идентичны, верни: {"identical": true, "differences": []}.

Тебе строго запрещено писать что-либо кроме этого JSON — даже фразу «вот ответ на ваш запрос», markdown, комментарии или обёртки ```json.

Твоя задача:
1. Ты получаешь два фрагмента — каждый в формате image или txt.
2. Если формат image — распознай текст с изображения. Если txt — используй текст как есть, без преобразований.
3. Построчно сравни два полученных текста.
4. Если в строках есть различия — верни ответ в заданном JSON-формате с перечислением всех отличающихся строк."""

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
            return parsed
    except json.JSONDecodeError:
        logger.warning("Failed to parse Ollama JSON response")

    return None
