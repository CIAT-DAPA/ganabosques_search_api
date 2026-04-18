import unittest
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from src.auth.auth import LoginRequest, login


class TestAuthRouter(unittest.TestCase):

    @patch("src.auth.auth.requests.post")
    @patch("src.auth.auth.os.getenv")
    def test_login_returns_tokens_when_keycloak_response_is_successful(
        self,
        mock_getenv,
        mock_post,
    ):
        mock_getenv.side_effect = [
            "https://kc.example.com",
            "test-realm",
            "client-id",
            "client-secret",
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
        }
        mock_post.return_value = mock_response

        data = LoginRequest(username="john", password="secret")
        result = login(data)

        self.assertEqual(
            result,
            {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
            },
        )

        mock_post.assert_called_once_with(
            "https://kc.example.com/realms/test-realm/protocol/openid-connect/token",
            data={
                "grant_type": "password",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "username": "john",
                "password": "secret",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    @patch("src.auth.auth.requests.post")
    @patch("src.auth.auth.os.getenv")
    def test_login_raises_http_exception_when_keycloak_response_is_not_successful(
        self,
        mock_getenv,
        mock_post,
    ):
        mock_getenv.side_effect = [
            "https://kc.example.com",
            "test-realm",
            "client-id",
            "client-secret",
        ]

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"error": "invalid_grant"}
        mock_post.return_value = mock_response

        data = LoginRequest(username="john", password="wrong-pass")

        with self.assertRaises(HTTPException) as context:
            login(data)

        self.assertEqual(context.exception.status_code, 401)
        self.assertEqual(context.exception.detail, {"error": "invalid_grant"})


if __name__ == "__main__":
    unittest.main()