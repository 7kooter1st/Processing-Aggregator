import asyncio
import logging
import re
from dataclasses import dataclass, replace
from typing import Any

from app.services.hierarchical_diff import (
    DiffCandidate,
    HierarchicalDiffEngine,
    collapse_whitespace,
    normalize_for_alignment,
)
from app.services.ocr_store import OcrStore
from app.services.ollama_client import OllamaClient
from app.services.prompt_builder import (
    build_classification_messages,
    extract_classification_decisions,
)

logger = logging.getLogger(__name__)

_CATEGORIES = {
    "substantive",
    "technical",
    "alignment_error",
    "ocr_uncertain",
}
_TECHNICAL_TYPES = {
    "markdown",
    "numbering",
    "page_number",
    "list_marker",
    "header_footer",
    "dash",
    "line_wrap",
}
_TECHNICAL_LABELS_RU = {
    "markdown": "Markdown-разметка",
    "numbering": "нумерация пункта",
    "page_number": "номер страницы",
    "list_marker": "маркер списка",
    "header_footer": "колонтитул или заголовочная область",
    "dash": "эквивалентный вид тире",
    "line_wrap": "перенос строки",
}
_DASHES = "‐‑‒–—−-"
_NUMBERING_RE = re.compile(r"^\s*\d+(?:\.\d+)*[.)]\s*$")
_LIST_MARKER_RE = re.compile(r"^\s*[•▪◦‣*–—-]\s*$")
_MARKDOWN_RE = re.compile(r"^\s*(?:#{1,6}|\*+|_+|`+)\s*$")
_DATE_RE = re.compile(
    r"\b(?:\d{1,2}[./-]\d{1,2}[./-]\d{2,4}|\d{4}[./-]\d{1,2}[./-]\d{1,2})\b"
)
_AMOUNT_RE = re.compile(
    r"\b\d[\d\s]*(?:[.,]\d+)?\s*(?:₽|руб(?:\.|лей)?|коп(?:\.|еек)?|USD|EUR)\b",
    re.IGNORECASE,
)
_ABBREVIATION_RE = re.compile(r"(?<!\w)[A-ZА-ЯЁ]{2,8}(?!\w)")


@dataclass(frozen=True)
class ClassificationDecision:
    candidate_id: str
    category: str
    technical_type: str | None
    reason: str
    confidence: float
    protection_tags: tuple[str, ...] = ()
    classified_by: str = "llm"
    include_in_result: bool = True

    def persistence_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "category": self.category,
            "technical_type": self.technical_type,
            "reason": self.reason,
            "confidence": self.confidence,
            "protection_tags": list(self.protection_tags),
            "classified_by": self.classified_by,
            "include_in_result": self.include_in_result,
        }


@dataclass(frozen=True)
class ClassifiedCandidate:
    candidate: DiffCandidate
    decision: ClassificationDecision

    def result_dict(self) -> dict[str, Any]:
        return self.candidate.result_dict(
            category=self.decision.category,
            technical_type=self.decision.technical_type,
            reason=self.decision.reason,
            confidence=self.decision.confidence,
            protection_tags=self.decision.protection_tags,
        )


class DifferenceClassifier:
    """Hybrid deterministic/LLM classifier with fail-visible semantics."""

    def __init__(self, *, ollama: OllamaClient, store: OcrStore) -> None:
        self._ollama = ollama
        self._store = store

    async def classify(
        self,
        candidates: list[DiffCandidate],
        *,
        run_id: str,
        file1_name: str,
        file2_name: str,
    ) -> tuple[list[ClassifiedCandidate], dict[str, int]]:
        if not candidates:
            return [], {
                "classification_batches": 0,
                "classification_failures": 0,
                "deterministic_technical": 0,
                "llm_classified": 0,
                "ocr_uncertain": 0,
            }

        output: list[ClassifiedCandidate] = []
        persistence: list[ClassificationDecision] = []
        pending: list[DiffCandidate] = []

        for group in self._group_by_alignment(candidates):
            group_decision = self._technical_group_decision(group)
            if group_decision is None:
                for candidate in group:
                    decision = self._technical_candidate_decision(candidate)
                    if decision is None:
                        pending.append(candidate)
                    else:
                        display = self._display_for_technical(candidate, decision)
                        output.append(ClassifiedCandidate(display, decision))
                        persistence.append(decision)
                continue

            representative = self._merge_technical_group(group)
            representative_decision = replace(
                group_decision,
                candidate_id=representative.candidate_id,
                include_in_result=True,
            )
            output.append(
                ClassifiedCandidate(representative, representative_decision)
            )
            persistence.append(representative_decision)
            for duplicate in group[1:]:
                persistence.append(
                    ClassificationDecision(
                        candidate_id=duplicate.candidate_id,
                        category="technical",
                        technical_type=group_decision.technical_type,
                        reason=(
                            "Объединено с техническим кандидатом "
                            f"{representative.candidate_id}"
                        ),
                        confidence=1.0,
                        classified_by="deterministic",
                        include_in_result=False,
                    )
                )

        pending, duplicate_decisions = self._dedupe_pending(pending)
        persistence.extend(duplicate_decisions)

        llm_decisions, batch_count, failures = await self._classify_pending(
            pending,
            run_id=run_id,
            file1_name=file1_name,
            file2_name=file2_name,
            first_batch_index=1,
            alignment_retry=False,
        )

        alignment_retry = [
            candidate
            for candidate in pending
            if llm_decisions[candidate.candidate_id].category
            == "alignment_error"
        ]
        if alignment_retry:
            expanded = [self._expanded_alignment_candidate(c) for c in alignment_retry]
            retry_decisions, retry_batches, retry_failures = (
                await self._classify_pending(
                    expanded,
                    run_id=run_id,
                    file1_name=file1_name,
                    file2_name=file2_name,
                    first_batch_index=batch_count + 1,
                    alignment_retry=True,
                )
            )
            batch_count += retry_batches
            failures += retry_failures
            for candidate in alignment_retry:
                retry = retry_decisions[candidate.candidate_id]
                if retry.classified_by == "llm":
                    retry = replace(retry, classified_by="alignment_retry")
                if retry.category == "alignment_error":
                    retry = ClassificationDecision(
                        candidate_id=candidate.candidate_id,
                        category="ocr_uncertain",
                        technical_type=None,
                        reason=(
                            "Повторное сопоставление с соседними блоками "
                            "не устранило ошибку выравнивания"
                        ),
                        confidence=retry.confidence,
                        classified_by="alignment_retry",
                    )
                llm_decisions[candidate.candidate_id] = retry

        for candidate in pending:
            guarded = self._apply_safety_gate(
                candidate,
                llm_decisions[candidate.candidate_id],
            )
            output.append(ClassifiedCandidate(candidate, guarded))
            persistence.append(guarded)

        await self._store.update_candidate_classifications(
            run_id,
            [item.persistence_dict() for item in persistence],
        )

        order = {
            candidate.candidate_id: index
            for index, candidate in enumerate(candidates)
        }
        output.sort(key=lambda item: order.get(item.candidate.candidate_id, 0))
        return output, {
            "classification_batches": batch_count,
            "classification_failures": failures,
            "deterministic_technical": sum(
                1
                for item in output
                if item.decision.category == "technical"
                and item.decision.classified_by == "deterministic"
            ),
            "llm_classified": sum(
                1
                for item in output
                if item.decision.classified_by in {"llm", "alignment_retry"}
            ),
            "ocr_uncertain": sum(
                1
                for item in output
                if item.decision.category == "ocr_uncertain"
            ),
        }

    async def _classify_pending(
        self,
        candidates: list[DiffCandidate],
        *,
        run_id: str,
        file1_name: str,
        file2_name: str,
        first_batch_index: int,
        alignment_retry: bool,
    ) -> tuple[dict[str, ClassificationDecision], int, int]:
        decisions: dict[str, ClassificationDecision] = {}
        failures = 0
        batches = HierarchicalDiffEngine.batch_for_classification(candidates)
        if batches:
            logger.info(
                "[CLASSIFY] run=%s batches=%s candidates=%s retry=%s",
                run_id,
                len(batches),
                len(candidates),
                alignment_retry,
            )

        for offset, batch in enumerate(batches):
            batch_index = first_batch_index + offset
            logger.info(
                "[CLASSIFY] batch %s/%s size=%s — ожидайте ответ Ollama",
                offset + 1,
                len(batches),
                len(batch),
            )
            payload = [candidate.classifier_dict() for candidate in batch]
            messages = build_classification_messages(
                file1_name=file1_name,
                file2_name=file2_name,
                candidates=payload,
                alignment_retry=alignment_retry,
            )
            await self._store.record_classification_batch(
                run_id,
                batch_index=batch_index,
                candidate_ids=[item.candidate_id for item in batch],
                request_data={"messages": messages},
            )

            loop = asyncio.get_running_loop()
            started = loop.time()
            response: dict[str, Any] | None = None
            try:
                response = await self._ollama.chat_json(messages)
                raw = extract_classification_decisions(response)
                parsed = self._validate_batch(raw, batch)
                parse_ok = parsed is not None
                if parsed is None:
                    failures += 1
                    parsed = self._fallback_decisions(
                        batch,
                        "LLM вернула неполный или некорректный JSON",
                    )
                decisions.update(parsed)
                failure_reason = None if parse_ok else "invalid_response"
            except Exception as exc:
                failures += 1
                parse_ok = False
                failure_reason = f"{type(exc).__name__}: {exc}"[:500]
                decisions.update(
                    self._fallback_decisions(
                        batch,
                        "Классификатор LLM недоступен; требуется проверка OCR",
                    )
                )

            latency_ms = round((loop.time() - started) * 1000)
            await self._store.complete_classification_batch(
                run_id,
                batch_index=batch_index,
                response_data=response,
                parse_ok=parse_ok,
                failure_reason=failure_reason,
                latency_ms=latency_ms,
            )

        return decisions, len(batches), failures

    @staticmethod
    def _validate_batch(
        raw: list[dict[str, Any]] | None,
        batch: list[DiffCandidate],
    ) -> dict[str, ClassificationDecision] | None:
        if raw is None or len(raw) != len(batch):
            return None

        expected = {candidate.candidate_id: candidate for candidate in batch}
        seen: set[str] = set()
        result: dict[str, ClassificationDecision] = {}

        for item in raw:
            if not isinstance(item, dict):
                return None
            candidate_id = item.get("candidate_id")
            if (
                not isinstance(candidate_id, str)
                or candidate_id not in expected
                or candidate_id in seen
            ):
                return None

            category = item.get("category")
            if category not in _CATEGORIES:
                return None
            technical_type = item.get("technical_type")
            if technical_type is not None and technical_type not in _TECHNICAL_TYPES:
                return None

            candidate = expected[candidate_id]
            payload = candidate.classifier_dict()["changed"]
            if item.get("left_change") != payload["file1"]:
                return None
            if item.get("right_change") != payload["file2"]:
                return None

            reason = item.get("reason")
            confidence = item.get("confidence")
            if not isinstance(reason, str) or not reason.strip():
                return None
            if not isinstance(confidence, (int, float)) or isinstance(
                confidence,
                bool,
            ):
                return None

            seen.add(candidate_id)
            result[candidate_id] = ClassificationDecision(
                candidate_id=candidate_id,
                category=category,
                technical_type=technical_type,
                reason=reason.strip()[:500],
                confidence=max(0.0, min(1.0, float(confidence))),
                classified_by="llm",
            )

        return result if seen == set(expected) else None

    @staticmethod
    def _fallback_decisions(
        batch: list[DiffCandidate],
        reason: str,
    ) -> dict[str, ClassificationDecision]:
        return {
            candidate.candidate_id: ClassificationDecision(
                candidate_id=candidate.candidate_id,
                category="ocr_uncertain",
                technical_type=None,
                reason=reason,
                confidence=0.0,
                classified_by="fallback",
            )
            for candidate in batch
        }

    def _apply_safety_gate(
        self,
        candidate: DiffCandidate,
        decision: ClassificationDecision,
    ) -> ClassificationDecision:
        protection_tags = self._protection_tags(candidate)
        if decision.category != "technical":
            return replace(decision, protection_tags=protection_tags)
        if protection_tags:
            return ClassificationDecision(
                candidate_id=candidate.candidate_id,
                category="substantive",
                technical_type=None,
                reason=(
                    "Защитный фильтр: изменение слов, чисел, дат, сумм "
                    "или отрицания нельзя скрыть как техническое"
                ),
                confidence=decision.confidence,
                protection_tags=protection_tags,
                classified_by="safety_override",
            )
        return ClassificationDecision(
            candidate_id=candidate.candidate_id,
            category="ocr_uncertain",
            technical_type=None,
            reason=(
                "LLM предложила скрыть изменение без детерминированного "
                "доказательства технического равенства"
            ),
            confidence=decision.confidence,
            protection_tags=protection_tags,
            classified_by="safety_override",
        )

    def _technical_group_decision(
        self,
        group: list[DiffCandidate],
    ) -> ClassificationDecision | None:
        first = group[0]
        if not first.evidence1 or not first.evidence2:
            return None
        if normalize_for_alignment(first.evidence1) != normalize_for_alignment(
            first.evidence2
        ):
            return None
        technical_type = self._technical_type(
            first.evidence1,
            first.evidence2,
        )
        return ClassificationDecision(
            candidate_id=first.candidate_id,
            category="technical",
            technical_type=technical_type,
            reason="Абзацы равны после детерминированной нормализации оформления",
            confidence=1.0,
            classified_by="deterministic",
        )

    def _technical_candidate_decision(
        self,
        candidate: DiffCandidate,
    ) -> ClassificationDecision | None:
        left = candidate.changed1.strip()
        right = candidate.changed2.strip()
        nonempty = left or right

        technical_type: str | None = None
        if self._dash_equivalent(left, right):
            technical_type = "dash"
        elif self._is_numbering(left) or self._is_numbering(right):
            technical_type = "numbering"
        elif candidate.change_position == "start" and (
            _LIST_MARKER_RE.fullmatch(nonempty) is not None
        ):
            technical_type = "list_marker"
        elif _MARKDOWN_RE.fullmatch(nonempty) is not None:
            technical_type = "markdown"
        elif self._is_page_number(candidate):
            technical_type = "page_number"
        elif self._is_header_artifact(candidate):
            technical_type = "header_footer"
        elif left and right and normalize_for_alignment(left) == normalize_for_alignment(
            right
        ):
            technical_type = self._technical_type(left, right)

        if technical_type is None:
            return None
        return ClassificationDecision(
            candidate_id=candidate.candidate_id,
            category="technical",
            technical_type=technical_type,
            reason=(
                "Детерминированно подтверждено: "
                f"{_TECHNICAL_LABELS_RU[technical_type]}"
            ),
            confidence=1.0,
            classified_by="deterministic",
        )

    @staticmethod
    def _group_by_alignment(
        candidates: list[DiffCandidate],
    ) -> list[list[DiffCandidate]]:
        groups: list[list[DiffCandidate]] = []
        current: list[DiffCandidate] = []
        current_id: int | None = None
        for candidate in candidates:
            if current and candidate.alignment_id != current_id:
                groups.append(current)
                current = []
            current.append(candidate)
            current_id = candidate.alignment_id
        if current:
            groups.append(current)
        return groups

    @staticmethod
    def _display_for_technical(
        candidate: DiffCandidate,
        decision: ClassificationDecision,
    ) -> DiffCandidate:
        if decision.technical_type not in {
            "numbering",
            "list_marker",
            "markdown",
            "page_number",
        }:
            return candidate
        left = candidate.changed1.strip()
        right = candidate.changed2.strip()
        return replace(
            candidate,
            file1_line=left[:200] if left else None,
            file2_line=right[:200] if right else None,
        )

    @classmethod
    def _dedupe_pending(
        cls,
        pending: list[DiffCandidate],
    ) -> tuple[list[DiffCandidate], list[ClassificationDecision]]:
        kept: list[DiffCandidate] = []
        duplicates: list[ClassificationDecision] = []
        for group in cls._group_by_alignment(pending):
            unique: list[DiffCandidate] = []
            for candidate in group:
                original = next(
                    (
                        existing
                        for existing in unique
                        if cls._same_change(existing, candidate)
                    ),
                    None,
                )
                if original is None:
                    unique.append(candidate)
                    continue
                duplicates.append(
                    ClassificationDecision(
                        candidate_id=candidate.candidate_id,
                        category="substantive",
                        technical_type=None,
                        reason=(
                            "Объединено с кандидатом "
                            f"{original.candidate_id} в том же абзаце"
                        ),
                        confidence=1.0,
                        classified_by="deterministic",
                        include_in_result=False,
                    )
                )
            kept.extend(unique)
        return kept, duplicates

    @staticmethod
    def _same_change(left: DiffCandidate, right: DiffCandidate) -> bool:
        left1 = collapse_whitespace(left.changed1).casefold()
        left2 = collapse_whitespace(left.changed2).casefold()
        right1 = collapse_whitespace(right.changed1).casefold()
        right2 = collapse_whitespace(right.changed2).casefold()
        if left1 == right1 and left2 == right2:
            return True

        def contained(first: str, second: str) -> bool:
            if not first or not second:
                return bool(first) == bool(second)
            if first == second:
                return True
            shorter, longer = (
                (first, second) if len(first) <= len(second) else (second, first)
            )
            if shorter.isdigit() and len(shorter) <= 4:
                return False
            return shorter in longer

        return contained(left1, right1) and contained(left2, right2)

    @staticmethod
    def _merge_technical_group(
        group: list[DiffCandidate],
    ) -> DiffCandidate:
        first = group[0]
        if len(group) == 1:
            return first
        tags = tuple(sorted({tag for item in group for tag in item.change_tags}))
        return replace(
            first,
            file1_line=(
                first.evidence1[:199] + "…"
                if len(first.evidence1) > 200
                else first.evidence1
            )
            or None,
            file2_line=(
                first.evidence2[:199] + "…"
                if len(first.evidence2) > 200
                else first.evidence2
            )
            or None,
            changed1=" ".join(item.changed1 for item in group if item.changed1),
            changed2=" ".join(item.changed2 for item in group if item.changed2),
            change_tags=tags,
        )

    @staticmethod
    def _expanded_alignment_candidate(candidate: DiffCandidate) -> DiffCandidate:
        def joined(previous: str, current: str, following: str) -> str:
            return collapse_whitespace(
                " ".join(part for part in (previous, current, following) if part)
            )[:4000]

        return replace(
            candidate,
            evidence1=joined(
                candidate.previous1,
                candidate.evidence1,
                candidate.next1,
            ),
            evidence2=joined(
                candidate.previous2,
                candidate.evidence2,
                candidate.next2,
            ),
        )

    @staticmethod
    def _technical_type(left: str, right: str) -> str:
        if DifferenceClassifier._dash_equivalent(left, right):
            return "dash"
        if "**" in left + right or "#" in left + right:
            return "markdown"
        if _NUMBERING_RE.search(left) or _NUMBERING_RE.search(right):
            return "numbering"
        if collapse_whitespace(left) == collapse_whitespace(right):
            return "line_wrap"
        return "numbering"

    @staticmethod
    def _dash_equivalent(left: str, right: str) -> bool:
        if left == right or not (left or right):
            return False
        translation = str.maketrans({char: "-" for char in _DASHES})
        return left.translate(translation) == right.translate(translation)

    @staticmethod
    def _is_numbering(value: str) -> bool:
        return bool(value and _NUMBERING_RE.fullmatch(value))

    @staticmethod
    def _is_page_number(candidate: DiffCandidate) -> bool:
        value = (candidate.changed1 or candidate.changed2).strip()
        if not value.isdigit() or candidate.change_position != "whole_block":
            return False
        evidence_values = [
            collapse_whitespace(value)
            for value in (candidate.evidence1, candidate.evidence2)
            if collapse_whitespace(value)
        ]
        if not evidence_values or not all(item.isdigit() for item in evidence_values):
            return False

        def at_edge(block: int | None, count: int | None) -> bool:
            if block is None or count is None:
                return False
            return block <= 2 or block >= max(1, count - 1)

        return at_edge(
            candidate.file1_block,
            candidate.file1_page_block_count,
        ) or at_edge(
            candidate.file2_block,
            candidate.file2_page_block_count,
        )

    @staticmethod
    def _is_header_artifact(candidate: DiffCandidate) -> bool:
        changed = (candidate.changed1 or candidate.changed2).strip()
        if candidate.change_position != "whole_block" or len(changed) > 80:
            return False
        block = candidate.file1_block or candidate.file2_block
        page = candidate.file1_page or candidate.file2_page
        if page != 1 or block is None or block > 3:
            return False
        if changed and all(not char.isalnum() for char in changed):
            return True
        letters = [char for char in changed if char.isalpha()]
        uppercase = (
            bool(letters)
            and sum(char.isupper() for char in letters) / len(letters) >= 0.8
        )
        other_context = " ".join(
            (
                candidate.evidence1,
                candidate.evidence2,
                candidate.previous1,
                candidate.previous2,
                candidate.next1,
                candidate.next2,
            )
        )
        repeated_elsewhere = changed.casefold() in other_context.casefold().replace(
            changed.casefold(),
            "",
            1,
        )
        asymmetric_extraction = (
            candidate.file1_was_ocr != candidate.file2_was_ocr
        )
        return uppercase and (repeated_elsewhere or asymmetric_extraction)

    @staticmethod
    def _protection_tags(candidate: DiffCandidate) -> tuple[str, ...]:
        tags: set[str] = set()
        left = candidate.changed1
        right = candidate.changed2
        combined = f"{left} {right}"
        left_words = re.findall(r"[A-Za-zА-Яа-яЁё]+", left)
        right_words = re.findall(r"[A-Za-zА-Яа-яЁё]+", right)

        folded_left = {word.casefold() for word in left_words}
        folded_right = {word.casefold() for word in right_words}
        if {"не", "без"} & (folded_left ^ folded_right):
            tags.add("negation")
        if left_words != right_words and (left_words or right_words):
            tags.add("word_change")
        if any(char.isdigit() for char in combined) and not (
            DifferenceClassifier._is_numbering(left)
            or DifferenceClassifier._is_numbering(right)
        ):
            tags.add("body_number")
        if _DATE_RE.search(combined):
            tags.add("date")
        if _AMOUNT_RE.search(combined):
            tags.add("amount")
        if _ABBREVIATION_RE.search(combined):
            tags.add("abbreviation")
        if len(left_words) > 1 or len(right_words) > 1:
            tags.add("phrase")
        return tuple(sorted(tags))
