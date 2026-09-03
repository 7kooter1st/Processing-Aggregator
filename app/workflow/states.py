TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "deleted"}
)
NON_TERMINAL_STATUSES = frozenset(
    {
        "queued",
        "preparing",
        "processing",
        "ocr_ready",
        "comparing",
        "classifying",
        "finalizing",
        "cancel_requested",
        "deleting",
    }
)

ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "queued": frozenset({"preparing", "processing", "failed", "cancel_requested"}),
    "preparing": frozenset({"queued", "processing", "failed", "cancel_requested"}),
    "processing": frozenset({"ocr_ready", "failed", "cancel_requested"}),
    "ocr_ready": frozenset({"comparing", "failed", "cancel_requested"}),
    "comparing": frozenset({"classifying", "finalizing", "completed", "failed", "cancel_requested"}),
    "classifying": frozenset({"finalizing", "completed", "failed", "cancel_requested"}),
    "finalizing": frozenset({"completed", "failed"}),
    "cancel_requested": frozenset({"cancelled", "deleting"}),
    "completed": frozenset({"deleting"}),
    "failed": frozenset({"deleting"}),
    "cancelled": frozenset({"deleting"}),
    "deleting": frozenset({"deleted"}),
    "deleted": frozenset(),
}


def can_transition(current: str, target: str) -> bool:
    if current in TERMINAL_STATUSES and target not in ALLOWED_TRANSITIONS.get(current, frozenset()):
        return False
    allowed = ALLOWED_TRANSITIONS.get(current)
    if allowed is None:
        return target == current
    return target == current or target in allowed
