import unittest
from unittest.mock import Mock

from instagrapi.exceptions import FeedbackRequired, LoginRequired

from src.auth.autenticacao import (
    CompatibleInstagramClient,
    PROJECT_ROOT,
    SESSION_FILE,
)


class AuthenticationTests(unittest.TestCase):
    def test_feedback_required_is_not_treated_as_expired_session(self) -> None:
        client = CompatibleInstagramClient()
        client.account_info = Mock(
            side_effect=FeedbackRequired("feedback_required: action limited")
        )

        with self.assertRaises(FeedbackRequired):
            client._is_session_valid()

    def test_login_required_marks_session_as_invalid(self) -> None:
        client = CompatibleInstagramClient()
        client.account_info = Mock(side_effect=LoginRequired("login_required"))

        self.assertFalse(client._is_session_valid())

    def test_default_session_is_saved_in_repository_root(self) -> None:
        self.assertEqual(SESSION_FILE, PROJECT_ROOT / "session.json")


if __name__ == "__main__":
    unittest.main()
