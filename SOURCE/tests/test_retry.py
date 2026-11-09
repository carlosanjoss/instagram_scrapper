import unittest

from src.utils.retry import RetryPolicy


class RetryClassificationTests(unittest.TestCase):
    def test_feedback_required_is_rate_limit(self) -> None:
        error = RuntimeError(
            "feedback_required: We limit how often you can do certain things"
        )

        self.assertTrue(RetryPolicy.is_rate_limited(error))
        self.assertTrue(RetryPolicy.is_fatal(error))
        self.assertFalse(RetryPolicy.is_login_required(error))

    def test_login_required_is_not_rate_limit(self) -> None:
        error = RuntimeError("login_required")

        self.assertTrue(RetryPolicy.is_login_required(error))
        self.assertFalse(RetryPolicy.is_rate_limited(error))

    def test_classification_follows_chained_exception(self) -> None:
        inner = RuntimeError("please wait a few minutes")
        outer = RuntimeError("comment collection failed")
        outer.__cause__ = inner

        self.assertTrue(RetryPolicy.is_rate_limited(outer))


if __name__ == "__main__":
    unittest.main()
