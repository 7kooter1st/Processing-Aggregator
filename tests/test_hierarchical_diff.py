import unittest

from app.services.hierarchical_diff import (
    HierarchicalDiffEngine,
    SourcePage,
    normalize_for_alignment,
)


class HierarchicalDiffEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = HierarchicalDiffEngine()

    def test_ignores_only_technical_whitespace_and_line_wrap(self) -> None:
        left = "Оплата  произведена\r\nв полном объёме."
        right = "Оплата произведена в полном объёме."

        self.assertEqual(self.engine.compare(left, right), [])
        self.assertEqual(
            self.engine.compare("Условия дого\u00ad\nвора.", "Условия договора."),
            [],
        )

    def test_reports_digit_punctuation_and_case_changes(self) -> None:
        left = "Иванов: сумма 1000 руб."
        right = "иванов; сумма 1001 руб."

        candidates = self.engine.compare(left, right)
        combined = " ".join(
            f"{item.file1_line} {item.file2_line}" for item in candidates
        )

        self.assertGreaterEqual(len(candidates), 3)
        self.assertIn("Иванов", combined)
        self.assertIn("1001", combined)
        self.assertIn(";", combined)

    def test_reports_inserted_ne_with_context_on_both_sides(self) -> None:
        candidates = self.engine.compare(
            "Оплата произведена.",
            "Оплата не произведена.",
        )

        self.assertEqual(len(candidates), 1)
        self.assertIn("Оплата произведена", candidates[0].file1_line or "")
        self.assertIn("Оплата не произведена", candidates[0].file2_line or "")
        self.assertIn("prefix_ne", candidates[0].change_tags)

    def test_inserted_paragraph_does_not_shift_following_blocks(self) -> None:
        left = "РАЗДЕЛ 1\n\nПервый пункт.\n\nТретий пункт."
        right = (
            "РАЗДЕЛ 1\n\nПервый пункт.\n\n"
            "Второй пункт.\n\nТретий пункт."
        )

        candidates = self.engine.compare(left, right)

        self.assertEqual(len(candidates), 1)
        self.assertIsNone(candidates[0].file1_line)
        self.assertEqual(candidates[0].file2_line, "Второй пункт.")

    def test_table_row_change_is_local(self) -> None:
        left = "| Наименование | Сумма |\n| Услуга | 500 |"
        right = "| Наименование | Сумма |\n| Услуга | 700 |"

        candidates = self.engine.compare(left, right)

        self.assertEqual(len(candidates), 1)
        self.assertIn("500", candidates[0].file1_line or "")
        self.assertIn("700", candidates[0].file2_line or "")

    def test_alignment_key_ignores_markdown_and_clause_prefix_only(self) -> None:
        self.assertEqual(
            normalize_for_alignment("**1. ПРЕДМЕТ ДОГОВОРА**"),
            normalize_for_alignment("ПРЕДМЕТ ДОГОВОРА"),
        )
        self.assertNotEqual(
            normalize_for_alignment("114 рабочих дней"),
            normalize_for_alignment("115 рабочих дней"),
        )
        self.assertNotEqual(
            normalize_for_alignment("31.08.2026 договор подписан"),
            normalize_for_alignment("30.08.2026 договор подписан"),
        )

    def test_page_metadata_is_preserved_on_candidates(self) -> None:
        candidates = self.engine.compare_pages(
            [
                SourcePage(
                    page_number=3,
                    text="Номер Телефона указывается в Справке.",
                    source_content_type="text",
                    was_ocr=False,
                )
            ],
            [
                SourcePage(
                    page_number=3,
                    text="1.6. Номер Договора указывается в Справке.",
                    source_content_type="image",
                    was_ocr=True,
                )
            ],
            candidate_prefix="job",
        )

        self.assertTrue(candidates)
        self.assertTrue(all(item.candidate_id.startswith("job:c") for item in candidates))
        self.assertTrue(all(item.file1_page == 3 for item in candidates))
        self.assertTrue(all(item.file2_page == 3 for item in candidates))
        self.assertTrue(all(item.file2_was_ocr for item in candidates))

    def test_body_number_inside_sentence_is_not_alignment_equivalent(self) -> None:
        candidates = self.engine.compare(
            "Общие правила (Приложение № 1 к настоящему Договору).",
            "Общие правила (Приложение № 2 к настоящему Договору).",
        )
        combined = " ".join(
            f"{item.changed1} {item.changed2}" for item in candidates
        )
        self.assertTrue(candidates)
        self.assertIn("1", combined)
        self.assertIn("2", combined)

    def test_one_to_many_alignment_keeps_two_numbered_items_together(self) -> None:
        left = (
            "Операции и остаток средств считаются подтвержденными\n"
            "Возвратить Банку чековые книжки с оставшимися чеками."
        )
        right = (
            "4.1.9. Операции и остаток средств считаются подтвержденными\n"
            "4.1.10. Возвратить Банку чековые книжки с оставшимися чеками."
        )

        candidates = self.engine.compare(left, right)

        self.assertTrue(candidates)
        self.assertEqual(len({item.alignment_id for item in candidates}), 1)
        self.assertTrue(
            all("Возвратить Банку" in item.evidence2 for item in candidates)
        )


if __name__ == "__main__":
    unittest.main()
