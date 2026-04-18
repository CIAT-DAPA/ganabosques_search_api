import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bson import DBRef, ObjectId
from fastapi import HTTPException

from src.routes.analisys_risk_router import (
    FarmRiskFilterRequest,
    get_farmrisk_filtered,
)


class TestAnalisysRiskRouter(unittest.TestCase):

    @patch("src.routes.analisys_risk_router.FarmRisk")
    def test_get_farmrisk_filtered_raises_http_exception_for_invalid_analysis_id(self, mock_farmrisk):
        data = FarmRiskFilterRequest(
            analysis_ids=["bad-id"],
            farm_ids=[str(ObjectId())],
        )

        with self.assertRaises(HTTPException) as context:
            get_farmrisk_filtered(data)

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid analysis_id", context.exception.detail)

    @patch("src.routes.analisys_risk_router.FarmRisk")
    def test_get_farmrisk_filtered_raises_http_exception_for_invalid_farm_id(self, mock_farmrisk):
        data = FarmRiskFilterRequest(
            analysis_ids=[str(ObjectId())],
            farm_ids=["bad-id"],
        )

        with self.assertRaises(HTTPException) as context:
            get_farmrisk_filtered(data)

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid farm_id", context.exception.detail)

    @patch("src.routes.analisys_risk_router.FarmRisk")
    def test_get_farmrisk_filtered_returns_empty_grouped_results_when_no_matches(self, mock_farmrisk):
        analysis_id = str(ObjectId())
        farm_id = str(ObjectId())

        qs = MagicMock()
        qs.no_dereference.return_value = qs
        qs.only.return_value = qs
        mock_farmrisk.objects.return_value = qs

        with patch("builtins.list", side_effect=lambda x: [] if x is qs else __import__("builtins").list(x)):
            data = FarmRiskFilterRequest(
                analysis_ids=[analysis_id],
                farm_ids=[farm_id],
            )
            result = get_farmrisk_filtered(data)

        self.assertEqual(result, {analysis_id: []})

    @patch("src.routes.analisys_risk_router.FarmRiskVerification")
    @patch("src.routes.analisys_risk_router.Adm3")
    @patch("src.routes.analisys_risk_router.Farm")
    @patch("src.routes.analisys_risk_router.FarmRisk")
    def test_get_farmrisk_filtered_returns_grouped_results_with_enrichment(
        self,
        mock_farmrisk,
        mock_farm,
        mock_adm3,
        mock_verification,
    ):
        analysis_oid = ObjectId()
        farm_oid = ObjectId()
        farmrisk_oid = ObjectId()
        adm3_oid = ObjectId()

        mock_farmrisk._get_collection_name.return_value = "farmrisk"

        farmrisk_doc = MagicMock()
        farmrisk_doc.id = farmrisk_oid
        farmrisk_doc.to_mongo.return_value.to_dict.return_value = {
            "_id": farmrisk_oid,
            "analysis_id": analysis_oid,
            "farm_id": farm_oid,
            "farm_polygons_id": ObjectId(),
            "risk_input": True,
            "risk_output": False,
            "risk_direct": True,
        }

        farmrisk_qs = MagicMock()
        farmrisk_qs.no_dereference.return_value = farmrisk_qs
        farmrisk_qs.only.return_value = farmrisk_qs
        farmrisk_qs.__iter__.return_value = iter([farmrisk_doc])
        mock_farmrisk.objects.return_value = farmrisk_qs

        farm_doc = MagicMock()
        farm_doc.id = farm_oid
        farm_doc.to_mongo.return_value.to_dict.return_value = {
            "_id": farm_oid,
            "adm3_id": adm3_oid,
        }
        farm_qs = MagicMock()
        farm_qs.no_dereference.return_value = farm_qs
        farm_qs.only.return_value = [farm_doc]
        mock_farm.objects.return_value = farm_qs

        adm3_doc = MagicMock()
        adm3_doc.id = adm3_oid
        adm3_doc.to_mongo.return_value.to_dict.return_value = {
            "_id": adm3_oid,
            "label": "ANTIOQUIA, MEDELLIN, VEREDA X",
        }
        adm3_qs = MagicMock()
        adm3_qs.only.return_value = [adm3_doc]
        mock_adm3.objects.return_value = adm3_qs

        verification_doc = MagicMock()
        verification_doc.to_mongo.return_value.to_dict.return_value = {
            "farmrisk": DBRef("farmrisk", farmrisk_oid),
            "user_id": ObjectId(),
            "verification": None,
            "observation": "ok",
            "status": True,
        }
        verification_qs = MagicMock()
        verification_qs.order_by.return_value = verification_qs
        verification_qs.only.return_value = [verification_doc]
        mock_verification.objects.return_value = verification_qs

        data = FarmRiskFilterRequest(
            analysis_ids=[str(analysis_oid)],
            farm_ids=[str(farm_oid)],
        )

        result = get_farmrisk_filtered(data)

        self.assertIn(str(analysis_oid), result)
        self.assertEqual(len(result[str(analysis_oid)]), 1)
        item = result[str(analysis_oid)][0]
        self.assertEqual(item["farm_id"], str(farm_oid))
        self.assertEqual(item["department"], "ANTIOQUIA")
        self.assertEqual(item["municipality"], "MEDELLIN")
        self.assertEqual(item["vereda"], "VEREDA X")
        self.assertEqual(item["verification"]["observation"], "ok")
        self.assertTrue(item["verification"]["status"])

    @patch("src.routes.analisys_risk_router.FarmRiskVerification")
    @patch("src.routes.analisys_risk_router.Adm3")
    @patch("src.routes.analisys_risk_router.Farm")
    @patch("src.routes.analisys_risk_router.FarmRisk")
    def test_get_farmrisk_filtered_returns_empty_verification_when_not_found(
        self,
        mock_farmrisk,
        mock_farm,
        mock_adm3,
        mock_verification,
    ):
        analysis_oid = ObjectId()
        farm_oid = ObjectId()
        farmrisk_oid = ObjectId()
        adm3_oid = ObjectId()

        mock_farmrisk._get_collection_name.return_value = "farmrisk"

        farmrisk_doc = MagicMock()
        farmrisk_doc.id = farmrisk_oid
        farmrisk_doc.to_mongo.return_value.to_dict.return_value = {
            "_id": farmrisk_oid,
            "analysis_id": analysis_oid,
            "farm_id": farm_oid,
        }

        farmrisk_qs = MagicMock()
        farmrisk_qs.no_dereference.return_value = farmrisk_qs
        farmrisk_qs.only.return_value = farmrisk_qs
        farmrisk_qs.__iter__.return_value = iter([farmrisk_doc])
        mock_farmrisk.objects.return_value = farmrisk_qs

        farm_doc = MagicMock()
        farm_doc.id = farm_oid
        farm_doc.to_mongo.return_value.to_dict.return_value = {
            "_id": farm_oid,
            "adm3_id": adm3_oid,
        }
        farm_qs = MagicMock()
        farm_qs.no_dereference.return_value = farm_qs
        farm_qs.only.return_value = [farm_doc]
        mock_farm.objects.return_value = farm_qs

        adm3_doc = MagicMock()
        adm3_doc.id = adm3_oid
        adm3_doc.to_mongo.return_value.to_dict.return_value = {
            "_id": adm3_oid,
            "label": "ANTIOQUIA, MEDELLIN, VEREDA X",
        }
        adm3_qs = MagicMock()
        adm3_qs.only.return_value = [adm3_doc]
        mock_adm3.objects.return_value = adm3_qs

        verification_qs = MagicMock()
        verification_qs.order_by.return_value = verification_qs
        verification_qs.only.return_value = []
        mock_verification.objects.return_value = verification_qs

        data = FarmRiskFilterRequest(
            analysis_ids=[str(analysis_oid)],
            farm_ids=[str(farm_oid)],
        )

        result = get_farmrisk_filtered(data)

        item = result[str(analysis_oid)][0]
        self.assertEqual(item["verification"], {})


if __name__ == "__main__":
    unittest.main()