import unittest

from app.workflow.states import TERMINAL_STATUSES, can_transition


class WorkflowStateTests(unittest.TestCase):
    def test_terminal_cannot_return_to_processing(self) -> None:
        self.assertFalse(can_transition("completed", "processing"))
        self.assertFalse(can_transition("failed", "queued"))
        self.assertFalse(can_transition("cancelled", "comparing"))

    def test_delete_saga_from_terminals(self) -> None:
        for status in ("completed", "failed", "cancelled"):
            self.assertTrue(can_transition(status, "deleting"))
        self.assertTrue(can_transition("deleting", "deleted"))
        self.assertFalse(can_transition("deleted", "queued"))

    def test_happy_path(self) -> None:
        self.assertTrue(can_transition("queued", "preparing"))
        self.assertTrue(can_transition("preparing", "processing"))
        self.assertTrue(can_transition("processing", "ocr_ready"))
        self.assertTrue(can_transition("ocr_ready", "comparing"))
        self.assertTrue(can_transition("comparing", "completed"))

    def test_terminal_set(self) -> None:
        self.assertIn("completed", TERMINAL_STATUSES)
        self.assertNotIn("comparing", TERMINAL_STATUSES)


if __name__ == "__main__":
    unittest.main()
