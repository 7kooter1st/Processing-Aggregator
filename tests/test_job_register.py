import unittest
from uuid import uuid4

from pydantic import ValidationError

from app.models.schemas import JobRegisterRequest


class JobRegisterRequestTests(unittest.TestCase):
    def test_requires_user_id(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            JobRegisterRequest(job_id="job-1")
        self.assertIn("user_id", str(ctx.exception))

    def test_accepts_user_id(self) -> None:
        user_id = uuid4()
        body = JobRegisterRequest(
            job_id="job-1",
            user_id=user_id,
            file1_name="a.pdf",
            file2_name="b.pdf",
        )
        self.assertEqual(body.user_id, user_id)
        self.assertEqual(body.file1_name, "a.pdf")
        self.assertEqual(body.file2_name, "b.pdf")
