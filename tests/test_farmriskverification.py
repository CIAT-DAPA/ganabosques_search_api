import unittest
from unittest.mock import MagicMock, patch

from bson import ObjectId
from fastapi import HTTPException

from src.routes.farmriskverification import (
    FarmRiskVerificationCreateRequest,
    create_farmrisk_verification,
)


class TestFarmRiskVerification(unittest.TestCase):

    @patch("src.routes.farmriskverification.FarmRisk")
    def test_create_farmrisk_verification_raises_for_invalid_farmrisk_id(self, mock_farmrisk):
        data = FarmRiskVerificationCreateRequest(
            farmrisk_id="bad-id",
            observation="obs",
            status=True,
        )

        with self.assertRaises(HTTPException) as context:
            create_farmrisk_verification(data, validation_result={"payload": {"user_db": {"id": str(ObjectId())}}})

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid farmrisk_id", context.exception.detail)

    @patch("src.routes.farmriskverification.FarmRisk")
    def test_create_farmrisk_verification_raises_when_farmrisk_not_found(self, mock_farmrisk):
        farmrisk_id = str(ObjectId())
        mock_farmrisk.objects.return_value.first.return_value = None

        data = FarmRiskVerificationCreateRequest(
            farmrisk_id=farmrisk_id,
            observation="obs",
            status=True,
        )

        with self.assertRaises(HTTPException) as context:
            create_farmrisk_verification(data, validation_result={"payload": {"user_db": {"id": str(ObjectId())}}})

        self.assertEqual(context.exception.status_code, 404)
        self.assertIn("FarmRisk not found", context.exception.detail)

    @patch("src.routes.farmriskverification.FarmRisk")
    def test_create_farmrisk_verification_raises_when_user_id_is_missing_in_token(self, mock_farmrisk):
        farmrisk_obj = MagicMock()
        mock_farmrisk.objects.return_value.first.return_value = farmrisk_obj

        data = FarmRiskVerificationCreateRequest(
            farmrisk_id=str(ObjectId()),
            observation="obs",
            status=True,
        )

        with self.assertRaises(HTTPException) as context:
            create_farmrisk_verification(data, validation_result={"payload": {"user_db": {}}})

        self.assertEqual(context.exception.status_code, 401)
        self.assertEqual(context.exception.detail, "User ID not found in token")

    @patch("src.routes.farmriskverification.User")
    @patch("src.routes.farmriskverification.FarmRisk")
    def test_create_farmrisk_verification_raises_when_user_not_found(
        self,
        mock_farmrisk,
        mock_user,
    ):
        user_id = str(ObjectId())
        farmrisk_obj = MagicMock()
        mock_farmrisk.objects.return_value.first.return_value = farmrisk_obj
        mock_user.objects.return_value.first.return_value = None

        data = FarmRiskVerificationCreateRequest(
            farmrisk_id=str(ObjectId()),
            observation="obs",
            status=True,
        )

        with self.assertRaises(HTTPException) as context:
            create_farmrisk_verification(data, validation_result={"payload": {"user_db": {"id": user_id}}})

        self.assertEqual(context.exception.status_code, 404)
        self.assertIn("User not found", context.exception.detail)

    @patch("src.routes.farmriskverification.UserVerifier")
    @patch("src.routes.farmriskverification.User")
    @patch("src.routes.farmriskverification.FarmRisk")
    def test_create_farmrisk_verification_raises_when_user_has_no_verification_rights(
        self,
        mock_farmrisk,
        mock_user,
        mock_userverifier,
    ):
        user_id = str(ObjectId())
        farmrisk_obj = MagicMock()
        user_obj = MagicMock()

        mock_farmrisk.objects.return_value.first.return_value = farmrisk_obj
        mock_user.objects.return_value.first.return_value = user_obj
        mock_userverifier.objects.return_value.first.return_value = None

        data = FarmRiskVerificationCreateRequest(
            farmrisk_id=str(ObjectId()),
            observation="obs",
            status=True,
        )

        with self.assertRaises(HTTPException) as context:
            create_farmrisk_verification(data, validation_result={"payload": {"user_db": {"id": user_id}}})

        self.assertEqual(context.exception.status_code, 403)
        self.assertIn("User not have verification rights", context.exception.detail)

    @patch("src.routes.farmriskverification.FarmRiskVerification")
    @patch("src.routes.farmriskverification.UserVerifier")
    @patch("src.routes.farmriskverification.User")
    @patch("src.routes.farmriskverification.FarmRisk")
    def test_create_farmrisk_verification_returns_expected_response(
        self,
        mock_farmrisk,
        mock_user,
        mock_userverifier,
        mock_verification,
    ):
        farmrisk_id = ObjectId()
        user_id = ObjectId()

        farmrisk_obj = MagicMock()
        farmrisk_obj.id = farmrisk_id

        user_obj = MagicMock()
        user_obj.id = user_id

        verification_obj = MagicMock()
        verification_obj.id = ObjectId()
        verification_obj.observation = "verified"
        verification_obj.status = True
        mock_verification.return_value = verification_obj

        mock_farmrisk.objects.return_value.first.return_value = farmrisk_obj
        mock_user.objects.return_value.first.return_value = user_obj
        mock_userverifier.objects.return_value.first.return_value = MagicMock()

        data = FarmRiskVerificationCreateRequest(
            farmrisk_id=str(farmrisk_id),
            observation="verified",
            status=True,
        )

        result = create_farmrisk_verification(
            data,
            validation_result={"payload": {"user_db": {"id": str(user_id)}}},
        )

        self.assertEqual(result.user_id, str(user_id))
        self.assertEqual(result.farmrisk_id, str(farmrisk_id))
        self.assertEqual(result.observation, "verified")
        self.assertTrue(result.status)
        verification_obj.save.assert_called_once()