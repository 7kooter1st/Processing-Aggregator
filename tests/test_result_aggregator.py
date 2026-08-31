import unittest

from app.services.result_aggregator import ResultAggregator


class FakeResultStore:
    def __init__(self, comparison=None) -> None:
        self.comparison = comparison
        self.calls = 0

    async def get_comparison_result(self, _job_id: str):
        self.calls += 1
        return self.comparison


class FakeHub:
    async def send_result(self, *_args, **_kwargs) -> None:
        return None


async def _publish_status(*_args, **_kwargs) -> None:
    return None


class ResultAggregatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_loads_persisted_content_equal_result(self) -> None:
        store = FakeResultStore(
            {
                "identical": False,
                "verdict": "content_equal",
                "differences": [
                    {
                        "candidate_id": "job:c00001",
                        "file1_line": "–",
                        "file2_line": "-",
                        "category": "technical",
                        "technical_type": "dash",
                    }
                ],
            }
        )
        aggregator = ResultAggregator(
            ws_hub=FakeHub(),
            publish_status=_publish_status,
            store=store,
        )

        first = await aggregator.get_result("job")
        second = await aggregator.get_result("job")

        self.assertIsNotNone(first)
        assert first is not None
        self.assertEqual(first.comparison.verdict.value, "content_equal")
        self.assertEqual(
            first.comparison.differences[0].category.value,
            "technical",
        )
        self.assertIs(first, second)
        self.assertEqual(store.calls, 1)

    def test_merge_preserves_different_verdict(self) -> None:
        comparison = ResultAggregator._merge_fragments(
            {
                1: {
                    "identical": False,
                    "verdict": "different",
                    "differences": [
                        {
                            "candidate_id": "job:c00001",
                            "file1_line": "не",
                            "file2_line": None,
                            "category": "substantive",
                        }
                    ],
                }
            },
            total_chunks=1,
        )

        self.assertFalse(comparison.identical)
        self.assertEqual(comparison.verdict.value, "different")
        self.assertEqual(len(comparison.differences), 1)


if __name__ == "__main__":
    unittest.main()
