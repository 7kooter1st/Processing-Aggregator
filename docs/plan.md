# Двухфазная обработка: OCR → Compare

> Архивный план первоначальной реализации. Текущий Processing сначала
> сохраняет OCR всех страниц в PostgreSQL, затем выполняет единый page-aware
> diff и гибридную классификацию кандидатов. Актуальная схема описана в
> `DEPLOYMENT_GUIDE.md`.

Processing всегда обрабатывает чанк в два шага:

1. **OCR** — если `content_type=image`, нейронка распознаёт текст.
2. **Compare** — оба фрагмента уже текст → нейронка сравнивает → JSON diffs.

Chunking не меняется. Отдельный сервис не вводится. Режимов/флагов нет.

---

## Поток

```text
Kafka raw_chunks
  → для каждого file: image → Ollama OCR → text; text → как есть
  → Ollama Compare (text ↔ text) → JSON
  → processed_results → aggregator → WS
```

---

## Изменения по файлам

### `app/services/prompt_builder.py`

- Разделить на два system-промпта:
  - `SYSTEM_PROMPT_OCR` — только распознать текст со страницы. Таблицы → Markdown `| col |`. Без сравнения, без JSON, без комментариев. Числа/даты/ФИО буквально. Нечитаемое → `[неразборчиво]`.
  - `SYSTEM_PROMPT_COMPARE` — текущий compare-промпт **без** «если image — сначала OCR». Только text↔text, JSON как сейчас.
- Функции:
  - `build_ocr_messages(chunk) → messages` (+ `images`)
  - `build_compare_messages(...)` — оба файла только text
  - `extract_ocr_text(response) → str` (срезать \`\`\` если модель обернула)
  - `extract_comparison_fragment` — без изменений по смыслу

### `app/services/ollama_client.py`

- `chat_text(messages)` — OCR, **без** `format: json`
- `chat_json(messages)` — Compare, с `format: json` (текущее поведение)
- Temperature из конфига (низкая, ~0.0–0.1)

### `app/services/chunk_processor.py`

В `process()`:

1. Статус: `Распознавание chunk N/M...` (если есть хотя бы один image)
2. `file1 = await to_text(message.file1)`
3. `file2 = await to_text(message.file2)`  
   (`to_text`: text → вернуть; image → OCR → `ChunkContent(content_type=text, content=...)`; пустой OCR → ошибка чанка)
4. Статус: `Сравнение chunk N/M...`
5. Compare → `extract_comparison_fragment` → publish result (как сейчас)
6. Статус завершения — как сейчас

Если оба файла image — OCR можно через `asyncio.gather`.

### `app/config.py` / `.env.example`

- Убрать необходимость чего-либо кроме текущих `OLLAMA_*`.
- Снизить `CONSUMER_MAX_CONCURRENT` до `1` или `2` (на image-чанке два вызова модели).

### Не трогать

- Chunking
- Kafka-топики и формат `raw_chunks`
- `result_aggregator.py` (если `comparison_fragment` тот же)
- Фронтенд / схема ответа WS

### Документация

- Обновить `message-flow.md`: два вызова Ollama вместо одного.

---

## Промпты (кратко)

**OCR:** распознай страницу → только текст; таблицы Markdown; ничего не сравнивай и не объясняй.

**Compare:** сравни два текста → JSON `{ identical, differences }` по текущим IGNORE/REPORT правилам; картинок нет.

---

## Порядок работ

1. `ollama_client` — `chat_text` / `chat_json`
2. `prompt_builder` — два промпта + билдеры + `extract_ocr_text`
3. `chunk_processor` — `to_text` + двухшаговый `process`
4. `CONSUMER_MAX_CONCURRENT` → 1–2
5. Обновить `message-flow.md`
6. Прогнать DOCX + PDF-скан (с таблицей) end-to-end
