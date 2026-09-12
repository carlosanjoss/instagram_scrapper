import unittest
from unittest.mock import Mock, patch

from instagrapi import Client
from instagrapi.exceptions import UnknownError

from src.auth.autenticacao import (
    CURRENT_ANDROID_APP_PROFILE,
    CompatibleInstagramClient,
    PROJECT_ROOT,
    SESSION_FILE,
    _configure_current_app,
)


class AuthenticationTests(unittest.TestCase):
    def test_needs_upgrade_uses_caa_login_fallback(self) -> None:
        client = CompatibleInstagramClient()
        client._try_caa_login = Mock(return_value=True)
        client.login_flow = Mock()
        error = UnknownError(
            "Your version of Instagram is out of date.",
            error_type="needs_upgrade",
        )

        with patch.object(Client, "login", side_effect=error):
            logged = client.login("usuario", "senha")

        self.assertTrue(logged)
        client._try_caa_login.assert_called_once_with(error, verification_code="")
        client.login_flow.assert_called_once_with()

    def test_unrelated_unknown_error_is_preserved(self) -> None:
        client = CompatibleInstagramClient()
        error = UnknownError("erro diferente", error_type="outro")

        with patch.object(Client, "login", side_effect=error):
            with self.assertRaises(UnknownError):
                client.login("usuario", "senha")

    def test_updates_an_outdated_instagram_app_profile(self) -> None:
        client = CompatibleInstagramClient()

        _configure_current_app(client)

        self.assertEqual(
            client.device_settings["app_version"],
            CURRENT_ANDROID_APP_PROFILE["app_version"],
        )
        self.assertIn(CURRENT_ANDROID_APP_PROFILE["app_version"], client.user_agent)

    def test_default_session_is_saved_in_repository_root(self) -> None:
        self.assertEqual(SESSION_FILE, PROJECT_ROOT / "session.json")


if __name__ == "__main__":
    unittest.main()
