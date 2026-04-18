import unittest
from unittest.mock import MagicMock, patch

from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from jose import JWTError, ExpiredSignatureError

from src.auth.token_validation_router import validate_local_token


class TestTokenValidationRouter(unittest.TestCase):

    def _build_credentials(self, token="test-token"):
        return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)

    @patch("src.auth.token_validation_router.requests.get")
    @patch("src.auth.token_validation_router.jwt.get_unverified_header")
    @patch("src.auth.token_validation_router.os.getenv")
    def test_validate_local_token_raises_500_when_jwks_request_fails(
        self,
        mock_getenv,
        mock_get_unverified_header,
        mock_requests_get,
    ):
        mock_get_unverified_header.return_value = {"kid": "kid-1", "alg": "RS256"}
        mock_getenv.side_effect = [
            "https://kc.example.com",
            "test-realm",
            "client-id",
        ]

        response = MagicMock()
        response.status_code = 500
        mock_requests_get.return_value = response

        with self.assertRaises(HTTPException) as context:
            validate_local_token(self._build_credentials())

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(
            context.exception.detail,
            "Error fetching JWKS from Keycloak",
        )

    @patch("src.auth.token_validation_router.requests.get")
    @patch("src.auth.token_validation_router.jwt.get_unverified_header")
    @patch("src.auth.token_validation_router.os.getenv")
    def test_validate_local_token_raises_401_when_public_key_is_not_found(
        self,
        mock_getenv,
        mock_get_unverified_header,
        mock_requests_get,
    ):
        mock_get_unverified_header.return_value = {"kid": "missing-kid", "alg": "RS256"}
        mock_getenv.side_effect = [
            "https://kc.example.com",
            "test-realm",
            "client-id",
        ]

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"keys": [{"kid": "other-kid"}]}
        mock_requests_get.return_value = response

        with self.assertRaises(HTTPException) as context:
            validate_local_token(self._build_credentials())

        self.assertEqual(context.exception.status_code, 401)
        self.assertEqual(context.exception.detail, "Public key not found")

    @patch("src.auth.token_validation_router.serialize_user_permissions")
    @patch("src.auth.token_validation_router.User")
    @patch("src.auth.token_validation_router.jwt.decode")
    @patch("src.auth.token_validation_router.requests.get")
    @patch("src.auth.token_validation_router.jwt.get_unverified_header")
    @patch("src.auth.token_validation_router.os.getenv")
    def test_validate_local_token_returns_valid_payload_when_user_exists(
        self,
        mock_getenv,
        mock_get_unverified_header,
        mock_requests_get,
        mock_jwt_decode,
        mock_user_class,
        mock_serialize_user_permissions,
    ):
        mock_get_unverified_header.return_value = {"kid": "kid-1", "alg": "RS256"}
        mock_getenv.side_effect = [
            "https://kc.example.com",
            "test-realm",
            "client-id",
        ]

        jwks_response = MagicMock()
        jwks_response.status_code = 200
        jwks_response.json.return_value = {"keys": [{"kid": "kid-1", "kty": "RSA"}]}
        mock_requests_get.return_value = jwks_response

        mock_jwt_decode.return_value = {
            "sub": "ext-123",
            "preferred_username": "john",
            "realm_access": {"roles": ["user"]},
            "allowed-origins": ["*"],
            "resource_access": {"account": {}},
        }

        existing_user = MagicMock()
        mock_queryset = MagicMock()
        mock_queryset.first.return_value = existing_user
        mock_user_class.objects.return_value = mock_queryset

        mock_serialize_user_permissions.return_value = {
            "id": "user-id",
            "ext_id": "ext-123",
            "admin": False,
            "roles": [],
            "all_actions": [],
            "all_options": [],
        }

        result = validate_local_token(self._build_credentials())

        self.assertTrue(result["valid"])
        self.assertEqual(result["payload"]["sub"], "ext-123")
        self.assertEqual(result["payload"]["preferred_username"], "john")
        self.assertNotIn("realm_access", result["payload"])
        self.assertNotIn("allowed-origins", result["payload"])
        self.assertNotIn("resource_access", result["payload"])
        self.assertEqual(
            result["payload"]["user_db"]["ext_id"],
            "ext-123",
        )

    @patch("src.auth.token_validation_router.serialize_user_permissions")
    @patch("src.auth.token_validation_router.User")
    @patch("src.auth.token_validation_router.jwt.decode")
    @patch("src.auth.token_validation_router.requests.get")
    @patch("src.auth.token_validation_router.jwt.get_unverified_header")
    @patch("src.auth.token_validation_router.os.getenv")
    def test_validate_local_token_creates_user_when_it_does_not_exist(
        self,
        mock_getenv,
        mock_get_unverified_header,
        mock_requests_get,
        mock_jwt_decode,
        mock_user_class,
        mock_serialize_user_permissions,
    ):
        mock_get_unverified_header.return_value = {"kid": "kid-1", "alg": "RS256"}
        mock_getenv.side_effect = [
            "https://kc.example.com",
            "test-realm",
            "client-id",
        ]

        jwks_response = MagicMock()
        jwks_response.status_code = 200
        jwks_response.json.return_value = {"keys": [{"kid": "kid-1", "kty": "RSA"}]}
        mock_requests_get.return_value = jwks_response

        mock_jwt_decode.return_value = {
            "sub": "ext-new",
            "preferred_username": "new-user",
        }

        mock_queryset = MagicMock()
        mock_queryset.first.return_value = None
        mock_user_class.objects.return_value = mock_queryset

        created_user = MagicMock()
        mock_user_class.return_value = created_user

        mock_serialize_user_permissions.return_value = {
            "id": "created-id",
            "ext_id": "ext-new",
            "admin": False,
            "roles": [],
            "all_actions": [],
            "all_options": [],
        }

        result = validate_local_token(self._build_credentials())

        self.assertTrue(result["valid"])
        mock_user_class.assert_called_once_with(ext_id="ext-new", admin=False)
        created_user.save.assert_called_once()

    @patch("src.auth.token_validation_router.jwt.decode")
    @patch("src.auth.token_validation_router.requests.get")
    @patch("src.auth.token_validation_router.jwt.get_unverified_header")
    @patch("src.auth.token_validation_router.os.getenv")
    def test_validate_local_token_raises_400_when_sub_is_missing(
        self,
        mock_getenv,
        mock_get_unverified_header,
        mock_requests_get,
        mock_jwt_decode,
    ):
        mock_get_unverified_header.return_value = {"kid": "kid-1", "alg": "RS256"}
        mock_getenv.side_effect = [
            "https://kc.example.com",
            "test-realm",
            "client-id",
        ]

        jwks_response = MagicMock()
        jwks_response.status_code = 200
        jwks_response.json.return_value = {"keys": [{"kid": "kid-1", "kty": "RSA"}]}
        mock_requests_get.return_value = jwks_response

        mock_jwt_decode.return_value = {"preferred_username": "john"}

        with self.assertRaises(HTTPException) as context:
            validate_local_token(self._build_credentials())

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(
            context.exception.detail,
            "Token does not contain 'sub' field",
        )

    @patch("src.auth.token_validation_router.jwt.decode")
    @patch("src.auth.token_validation_router.requests.get")
    @patch("src.auth.token_validation_router.jwt.get_unverified_header")
    @patch("src.auth.token_validation_router.os.getenv")
    def test_validate_local_token_raises_401_when_token_is_expired(
        self,
        mock_getenv,
        mock_get_unverified_header,
        mock_requests_get,
        mock_jwt_decode,
    ):
        mock_get_unverified_header.return_value = {"kid": "kid-1", "alg": "RS256"}
        mock_getenv.side_effect = [
            "https://kc.example.com",
            "test-realm",
            "client-id",
        ]

        jwks_response = MagicMock()
        jwks_response.status_code = 200
        jwks_response.json.return_value = {"keys": [{"kid": "kid-1", "kty": "RSA"}]}
        mock_requests_get.return_value = jwks_response

        mock_jwt_decode.side_effect = ExpiredSignatureError()

        with self.assertRaises(HTTPException) as context:
            validate_local_token(self._build_credentials())

        self.assertEqual(context.exception.status_code, 401)
        self.assertEqual(context.exception.detail, "Expired token")

    @patch("src.auth.token_validation_router.jwt.decode")
    @patch("src.auth.token_validation_router.requests.get")
    @patch("src.auth.token_validation_router.jwt.get_unverified_header")
    @patch("src.auth.token_validation_router.os.getenv")
    def test_validate_local_token_raises_401_when_token_is_invalid(
        self,
        mock_getenv,
        mock_get_unverified_header,
        mock_requests_get,
        mock_jwt_decode,
    ):
        mock_get_unverified_header.return_value = {"kid": "kid-1", "alg": "RS256"}
        mock_getenv.side_effect = [
            "https://kc.example.com",
            "test-realm",
            "client-id",
        ]

        jwks_response = MagicMock()
        jwks_response.status_code = 200
        jwks_response.json.return_value = {"keys": [{"kid": "kid-1", "kty": "RSA"}]}
        mock_requests_get.return_value = jwks_response

        mock_jwt_decode.side_effect = JWTError("bad token")

        with self.assertRaises(HTTPException) as context:
            validate_local_token(self._build_credentials())

        self.assertEqual(context.exception.status_code, 401)
        self.assertIn("Invalid Token", context.exception.detail)


if __name__ == "__main__":
    unittest.main()