import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from src.auth.get_client_token import ClientCredentials, get_token


class TestGetClientTokenRouter(unittest.IsolatedAsyncioTestCase):

    @patch("src.auth.get_client_token.httpx.AsyncClient")
    @patch("src.auth.get_client_token.os.getenv")
    async def test_get_token_returns_json_when_keycloak_response_is_successful(
        self,
        mock_getenv,
        mock_async_client,
    ):
        mock_getenv.side_effect = [
            "https://kc.example.com",
            "test-realm",
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "client-token"}

        mock_client_instance = AsyncMock()
        mock_client_instance.post.return_value = mock_response

        mock_context_manager = AsyncMock()
        mock_context_manager.__aenter__.return_value = mock_client_instance
        mock_context_manager.__aexit__.return_value = None
        mock_async_client.return_value = mock_context_manager

        body = ClientCredentials(
            client_id="client-id",
            client_secret="client-secret",
        )

        result = await get_token(body)

        self.assertEqual(result, {"access_token": "client-token"})
        mock_client_instance.post.assert_awaited_once_with(
            "https://kc.example.com/realms/test-realm/protocol/openid-connect/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "client-id",
                "client_secret": "client-secret",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    @patch("src.auth.get_client_token.httpx.AsyncClient")
    @patch("src.auth.get_client_token.os.getenv")
    async def test_get_token_raises_http_exception_when_keycloak_response_is_not_successful(
        self,
        mock_getenv,
        mock_async_client,
    ):
        mock_getenv.side_effect = [
            "https://kc.example.com",
            "test-realm",
        ]

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "invalid client"

        mock_client_instance = AsyncMock()
        mock_client_instance.post.return_value = mock_response

        mock_context_manager = AsyncMock()
        mock_context_manager.__aenter__.return_value = mock_client_instance
        mock_context_manager.__aexit__.return_value = None
        mock_async_client.return_value = mock_context_manager

        body = ClientCredentials(
            client_id="client-id",
            client_secret="wrong-secret",
        )

        with self.assertRaises(HTTPException) as context:
            await get_token(body)

        self.assertEqual(context.exception.status_code, 401)
        self.assertEqual(
            context.exception.detail,
            "Credenciales inválidas o cliente no existe",
        )


if __name__ == "__main__":
    unittest.main()