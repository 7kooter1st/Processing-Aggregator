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
    if not isinstance(chunk, dict):
        return {"value": _truncate(str(chunk))}
    return {
        "filename": chunk.get("filename"),
        "format": chunk.get("format"),
        "content_type": chunk.get("content_type"),
        "content": _truncate(chunk.get("content", "")),
    }


def _sanitize_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively hide large file bodies in nested Kafka payloads."""
    out = dict(data)

    for key in ("file1", "file2"):
        if key in out:
            out[key] = _summarize_chunk_content(out.get(key))

    if "original" in out and isinstance(out["original"], dict):
        out["original"] = _sanitize_dict(out["original"])

    if "ollama" in out and isinstance(out["ollama"], dict):
        ollama = out["ollama"]
        message = (ollama or {}).get("message") or {}
        content = message.get("content", "")
        out["ollama"] = {
            "model": ollama.get("model"),
            "content_preview": _truncate(content),
            "content_len": len(content) if isinstance(content, str) else 0,
        }

    if "comparison_fragment" in out and out["comparison_fragment"]:
        frag = out["comparison_fragment"]
        diffs = frag.get("differences") or []
        out["comparison_fragment"] = {
            "identical": frag.get("identical"),
            "differences_count": len(diffs),
        }

    if "data" in out and isinstance(out["data"], dict):
        inner = dict(out["data"])
        if "comparison" in inner and isinstance(inner["comparison"], dict):
            comp = inner["comparison"]
            inner["comparison"] = {
                "identical": comp.get("identical"),
                "differences_count": len(comp.get("differences") or []),
            }
        out["data"] = inner

    if "error" in out and isinstance(out["error"], str):
        out["error"] = _truncate(out["error"], max_len=300)

    return out


def summarize_for_log(payload: dict[str, Any]) -> str:
    """Compact JSON for console — hides large base64/text bodies."""
    if not isinstance(payload, dict):
        return _truncate(str(payload), max_len=300)
    return json.dumps(_sanitize_dict(payload), ensure_ascii=False, default=str)


def strip_payload_for_dlt(payload: dict[str, Any]) -> dict[str, Any]:
    """Keep DLT small: replace image/text bodies with size metadata."""
    data = dict(payload)
    for key in ("file1", "file2"):
        part = data.get(key)
        if not isinstance(part, dict):
            continue
        content = part.get("content", "")
        data[key] = {
            "filename": part.get("filename"),
            "format": part.get("format"),
            "content_type": part.get("content_type"),
            "content_chars": len(content) if isinstance(content, str) else 0,
            "content": "[omitted]",
        }
    return data
