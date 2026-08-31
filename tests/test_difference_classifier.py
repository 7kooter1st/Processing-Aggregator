import json
import unittest

from app.services.difference_classifier import (
    ClassificationDecision,
    DifferenceClassifier,
)
from app.services.hierarchical_diff import DiffCandidate, HierarchicalDiffEngine


class FakeStore:
    def __init__(self) -> None:
        self.batches: list[dict] = []
        self.completed_batches: list[dict] = []
        self.classifications: list[dict] = []

    async def record_classification_batch(self, run_id: str, **values) -> None:
        self.batches.append({"run_id": run_id, **values})

    async def complete_classification_batch(self, run_id: str, **values) -> None:
        self.completed_batches.append({"run_id": run_id, **values})

    async def update_candidate_classifications(
        self,
        run_id: str,
        classifications: list[dict],
    ) -> None:
        self.classifications.extend(classifications)


class StaticOllama:
    def __init__(self, decisions: list[dict] | None = None) -> None:
        self.decisions = decisions
        self.calls = 0

    async def chat_json(self, _messages: list[dict]) -> dict:
        self.calls += 1
        payload = (
            {"unexpected": True}
            if self.decisions is None
            else {"decisions": self.decisions}
        )
        return {"message": {"content": json.dumps(payload, ensure_ascii=False)}}


class FailingOllama:
    async def chat_json(self, _messages: list[dict]) -> dict:
        raise TimeoutError("classification timeout")


class DifferenceClassifierTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.engine = HierarchicalDiffEngine()

    async def test_markdown_and_numbering_are_deterministic_technical(self) -> None:
        candidates = self.engine.compare(
            "ПРЕДМЕТ ДОГОВОРА",
            "**1. ПРЕДМЕТ ДОГОВОРА**",
            candidate_prefix="job",
        )
        ollama = StaticOllama()
        store = FakeStore()
        classifier = DifferenceClassifier(ollama=ollama, store=store)

        classified, summary = await classifier.classify(
            candidates,
            run_id="00000000-0000-0000-0000-000000000001",
            file1_name="one.docx",
            file2_name="two.pdf",
        )

        self.assertEqual(ollama.calls, 0)
        self.assertEqual(len(classified), 1)
        self.assertEqual(classified[0].decision.category, "technical")
        self.assertEqual(summary["deterministic_technical"], 1)

    async def test_valid_llm_word_replacement_remains_substantive(self) -> None:
        candidate = self.engine.compare(
            "Номер Телефона указывается в Справке.",
            "Номер Договора указывается в Справке.",
            candidate_prefix="job",
        )[0]
        changed = candidate.classifier_dict()["changed"]
        ollama = StaticOllama(
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "category": "substantive",
                    "technical_type": None,
                    "left_change": changed["file1"],
                    "right_change": changed["file2"],
                    "reason": "Заменено слово внутри предложения",
                    "confidence": 0.99,
                }
            ]
        )
        classifier = DifferenceClassifier(ollama=ollama, store=FakeStore())

        classified, _summary = await classifier.classify(
            [candidate],
            run_id="00000000-0000-0000-0000-000000000002",
            file1_name="one.docx",
            file2_name="two.pdf",
        )

        self.assertEqual(classified[0].decision.category, "substantive")
        self.assertIn(
            "word_change",
            classified[0].decision.protection_tags,
        )

    async def test_invalid_json_falls_back_to_visible_ocr_uncertain(self) -> None:
        candidate = self.engine.compare(
            "Заявителя",
            "Заявления",
            candidate_prefix="job",
        )[0]
        store = FakeStore()
        classifier = DifferenceClassifier(
            ollama=StaticOllama(),
            store=store,
        )

        classified, summary = await classifier.classify(
            [candidate],
            run_id="00000000-0000-0000-0000-000000000003",
            file1_name="one.docx",
            file2_name="two.pdf",
        )

        self.assertEqual(classified[0].decision.category, "ocr_uncertain")
        self.assertEqual(summary["classification_failures"], 1)
        self.assertFalse(store.completed_batches[0]["parse_ok"])

    async def test_timeout_falls_back_to_visible_ocr_uncertain(self) -> None:
        candidate = self.engine.compare(
            "114 рабочих дней",
            "115 рабочих дней",
            candidate_prefix="job",
        )[0]
        classifier = DifferenceClassifier(
            ollama=FailingOllama(),
            store=FakeStore(),
        )

        classified, _summary = await classifier.classify(
            [candidate],
            run_id="00000000-0000-0000-0000-000000000004",
            file1_name="one.docx",
            file2_name="two.pdf",
        )

        self.assertEqual(classified[0].decision.category, "ocr_uncertain")
        self.assertIn("body_number", classified[0].decision.protection_tags)

    def test_cross_candidate_or_missing_ids_reject_whole_batch(self) -> None:
        candidates = self.engine.compare(
            "Телефона и 114",
            "Договора и 115",
            candidate_prefix="job",
        )
        first = candidates[0]
        changed = first.classifier_dict()["changed"]
        raw = [
            {
                "candidate_id": "another-job:c99999",
                "category": "substantive",
                "technical_type": None,
                "left_change": changed["file1"],
                "right_change": changed["file2"],
                "reason": "Смешан ID",
                "confidence": 1.0,
            }
        ]

        self.assertIsNone(DifferenceClassifier._validate_batch(raw, candidates))

    def test_safety_gate_refuses_llm_technical_for_negation(self) -> None:
        candidate = self.engine.compare(
            "Клиент передает документ.",
            "Клиент не передает документ.",
            candidate_prefix="job",
        )[0]
        classifier = DifferenceClassifier(
            ollama=StaticOllama(),
            store=FakeStore(),
        )
        decision = ClassificationDecision(
            candidate_id=candidate.candidate_id,
            category="technical",
            technical_type="line_wrap",
            reason="Ошибочное решение модели",
            confidence=0.9,
        )

        guarded = classifier._apply_safety_gate(candidate, decision)

        self.assertEqual(guarded.category, "substantive")
        self.assertIn("negation", guarded.protection_tags)

    def test_unprotected_llm_technical_stays_visible_as_ocr_uncertain(self) -> None:
        candidate = self.engine.compare(
            "Текст (примечание).",
            "Текст /примечание/.",
            candidate_prefix="job",
        )[0]
        classifier = DifferenceClassifier(
            ollama=StaticOllama(),
            store=FakeStore(),
        )
        decision = ClassificationDecision(
            candidate_id=candidate.candidate_id,
            category="technical",
            technical_type="line_wrap",
            reason="Ошибочное решение модели",
            confidence=0.4,
        )

        guarded = classifier._apply_safety_gate(candidate, decision)

        self.assertEqual(guarded.category, "ocr_uncertain")
        self.assertEqual(guarded.classified_by, "safety_override")

    def test_clause_number_is_structural_but_plain_body_number_is_not(self) -> None:
        self.assertTrue(DifferenceClassifier._is_numbering("4.1.10."))
        self.assertFalse(DifferenceClassifier._is_numbering("115"))

    async def test_appendix_number_and_bez_are_protected(self) -> None:
        appendix = self.engine.compare(
            "Общие правила (Приложение № 1 к настоящему Договору).",
            "Общие правила (Приложение № 2 к настоящему Договору).",
            candidate_prefix="job",
        )[0]
        negation = self.engine.compare(
            "перевода денежных средств согласия Клиента",
            "перевода денежных средств без согласия Клиента",
            candidate_prefix="job",
        )[0]
        self.assertIn("body_number", DifferenceClassifier._protection_tags(appendix))
        self.assertIn("negation", DifferenceClassifier._protection_tags(negation))

    async def test_leading_clause_number_only_is_technical(self) -> None:
        candidates = self.engine.compare(
            "Предметом настоящего Договора является открытие Банком Клиенту Счета.",
            "1.1. Предметом настоящего Договора является открытие Банком Клиенту Счета.",
            candidate_prefix="job",
        )
        ollama = StaticOllama()
        classifier = DifferenceClassifier(ollama=ollama, store=FakeStore())

        classified, _summary = await classifier.classify(
            candidates,
            run_id="00000000-0000-0000-0000-000000000006",
            file1_name="one.docx",
            file2_name="two.pdf",
        )

        self.assertEqual(ollama.calls, 0)
        self.assertTrue(classified)
        self.assertTrue(
            all(item.decision.category == "technical" for item in classified)
        )
        self.assertTrue(
            all(item.decision.technical_type == "numbering" for item in classified)
        )

    async def test_dash_only_change_is_deterministic_technical(self) -> None:
        candidates = self.engine.compare(
            "расчетно–кассовое обслуживание",
            "расчетно-кассовое обслуживание",
            candidate_prefix="job",
        )
        ollama = StaticOllama()
        classifier = DifferenceClassifier(ollama=ollama, store=FakeStore())

        classified, summary = await classifier.classify(
            candidates,
            run_id="00000000-0000-0000-0000-000000000005",
            file1_name="one.docx",
            file2_name="two.pdf",
        )

        self.assertEqual(ollama.calls, 0)
        self.assertTrue(classified)
        self.assertTrue(
            all(item.decision.category == "technical" for item in classified)
        )
        self.assertGreaterEqual(summary["deterministic_technical"], 1)

    async def test_overlapping_same_paragraph_candidates_are_merged(self) -> None:
        left = DiffCandidate(
            line_number=None,
            file1_line="Телефона",
            file2_line="Договора",
            evidence1="Номер Телефона указывается в Справке",
            evidence2="Номер Договора указывается в Справке",
            change_tags=("literal", "word"),
            candidate_id="job:c00039",
            alignment_id=12,
            changed1="Телефона",
            changed2="Договора",
        )
        right = DiffCandidate(
            line_number=None,
            file1_line="Номер Телефона указывается",
            file2_line="Номер Договора указывается",
            evidence1="Номер Телефона указывается в Справке",
            evidence2="Номер Договора указывается в Справке",
            change_tags=("literal", "word"),
            candidate_id="job:c00040",
            alignment_id=12,
            changed1="Номер Телефона указывается",
            changed2="Номер Договора указывается",
        )

        kept, duplicates = DifferenceClassifier._dedupe_pending([left, right])

        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0].candidate_id, "job:c00039")
        self.assertEqual(len(duplicates), 1)
        self.assertEqual(duplicates[0].candidate_id, "job:c00040")
        self.assertFalse(duplicates[0].include_in_result)


if __name__ == "__main__":
    unittest.main()
