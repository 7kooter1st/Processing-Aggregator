import json
from typing import Any

_MAX_CONTENT_LEN = 120


def _truncate(value: Any, max_len: int = _MAX_CONTENT_LEN) -> Any:
    if isinstance(value, str) and len(value) > max_len:
        return f"{value[:max_len]}... ({len(value)} chars)"
    return value


def _summarize_chunk_content(chunk: dict[str, Any] | None) -> dict[str, Any] | None:
    if chunk is None:
        return None
    return {
        "filename": chunk.get("filename"),
        "format": chunk.get("format"),
        "content_type": chunk.get("content_type"),
        "content": _truncate(chunk.get("content", "")),
    }


def summarize_for_log(payload: dict[str, Any]) -> str:
    """Compact JSON for console — hides large base64/text bodies."""
    data = dict(payload)

    for key in ("file1", "file2"):
        if key in data:
            data[key] = _summarize_chunk_content(data.get(key))

    if "ollama" in data:
        ollama = data["ollama"]
        message = (ollama or {}).get("message") or {}
        content = message.get("content", "")
        data["ollama"] = {
            "model": ollama.get("model"),
            "content_preview": _truncate(content),
            "content_len": len(content) if isinstance(content, str) else 0,
        }

    if "comparison_fragment" in data and data["comparison_fragment"]:
        frag = data["comparison_fragment"]
        diffs = frag.get("differences") or []
        data["comparison_fragment"] = {
            "identical": frag.get("identical"),
            "differences_count": len(diffs),
        }

    if "data" in data and isinstance(data["data"], dict):
        inner = dict(data["data"])
        if "comparison" in inner:
            comp = inner["comparison"]
            inner["comparison"] = {
                "identical": comp.get("identical"),
                "differences_count": len(comp.get("differences") or []),
            }
        data["data"] = inner

    return json.dumps(data, ensure_ascii=False, default=str)
