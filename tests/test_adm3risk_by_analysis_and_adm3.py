import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bson import DBRef, ObjectId
from fastapi import HTTPException

from src.routes.adm3risk_by_analysis_and_adm3 import (
    Adm3RiskFilterRequest,
    _as_object_id,
    _safe_iso,
    get_adm3risk_filtered,
)


class DummyMongoWrapper:
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return self._data


class DummyDeforestation:
    def __init__(self, oid, period_start=None, period_end=None):
        self.id = oid
        self._period_start = period_start
        self._period_end = period_end

    def to_mongo(self):
        return DummyMongoWrapper(
            {
                "_id": self.id,
                "period_start": self._period_start,
                "period_end": self._period_end,
            }
        )


class TestAdm3RiskByAnalysisAndAdm3(unittest.TestCase):

    def test_as_object_id_returns_none_for_none(self):
        self.assertIsNone(_as_object_id(None))

    def test_as_object_id_returns_objectid_when_input_is_objectid(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id(oid), oid)

    def test_as_object_id_returns_objectid_when_input_is_dbref(self):
        oid = ObjectId()
        ref = DBRef("collection", oid)
        self.assertEqual(_as_object_id(ref), oid)

    def test_as_object_id_returns_none_for_invalid_string(self):
        self.assertIsNone(_as_object_id("bad-id"))

    def test_safe_iso_returns_none_for_none(self):
        self.assertIsNone(_safe_iso(None))

    def test_safe_iso_returns_none_when_value_has_no_isoformat(self):
        self.assertIsNone(_safe_iso("not-a-datetime"))

    def test_get_adm3risk_filtered_raises_http_exception_when_lists_are_missing(self):
        data = Adm3RiskFilterRequest(analysis_ids=[], adm3_ids=[])

        with self.assertRaises(HTTPException) as context:
            get_adm3risk_filtered(data)

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("analysis_ids y adm3_ids son requeridos", context.exception.detail)

    def test_get_adm3risk_filtered_raises_http_exception_when_ids_are_invalid(self):
        data = Adm3RiskFilterRequest(analysis_ids=["bad"], adm3_ids=["worse"])

        with self.assertRaises(HTTPException) as context:
            get_adm3risk_filtered(data)

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("IDs inválidos", context.exception.detail)

    @patch("src.routes.adm3risk_by_analysis_and_adm3.Adm3Risk")
    @patch("src.routes.adm3risk_by_analysis_and_adm3.Deforestation")
    @patch("src.routes.adm3risk_by_analysis_and_adm3.Analysis")
    def test_get_adm3risk_filtered_returns_grouped_results_with_existing_and_missing_pairs(
        self,
        mock_analysis,
        mock_deforestation,
        mock_adm3risk,
    ):
        analysis_oid = ObjectId()
        adm3_oid_1 = ObjectId()
        adm3_oid_2 = ObjectId()
        defo_oid = ObjectId()

        analysis_doc = SimpleNamespace(id=analysis_oid, deforestation_id=defo_oid)
        analysis_qs = MagicMock()
        analysis_qs.no_dereference.return_value = analysis_qs
        analysis_qs.only.return_value = [analysis_doc]
        mock_analysis.objects.return_value = analysis_qs

        deforestation_doc = DummyDeforestation(defo_oid)
        defo_qs = MagicMock()
        defo_qs.no_dereference.return_value = defo_qs
        defo_qs.only.return_value = [deforestation_doc]
        mock_deforestation.objects.return_value = defo_qs

        coll = MagicMock()
        coll.find.return_value = [
            {
                "analysis_id": analysis_oid,
                "adm3_id": adm3_oid_1,
                "risk_total": True,
                "def_ha": 10.5,
                "farm_amount": 3,
                "farm_total_amount": 7,
            }
        ]
        mock_adm3risk._get_collection.return_value = coll

        data = Adm3RiskFilterRequest(
            analysis_ids=[str(analysis_oid)],
            adm3_ids=[str(adm3_oid_1), str(adm3_oid_2)],
        )

        result = get_adm3risk_filtered(data)

        self.assertIn(str(analysis_oid), result)
        self.assertEqual(len(result[str(analysis_oid)]), 2)

        existing = result[str(analysis_oid)][0]
        missing = result[str(analysis_oid)][1]

        self.assertEqual(existing["adm3_id"], str(adm3_oid_1))
        self.assertTrue(existing["risk_total"])
        self.assertEqual(existing["farm_amount"], 3)
        self.assertEqual(existing["farm_total_amount"], 7)
        self.assertEqual(existing["def_ha"], 10.5)

        self.assertEqual(missing["adm3_id"], str(adm3_oid_2))
        self.assertFalse(missing["risk_total"])
        self.assertEqual(missing["farm_amount"], 0)
        self.assertEqual(missing["farm_total_amount"], 0)
        self.assertEqual(missing["def_ha"], 0.0)

    @patch("src.routes.adm3risk_by_analysis_and_adm3.Adm3Risk")
    @patch("src.routes.adm3risk_by_analysis_and_adm3.Deforestation")
    @patch("src.routes.adm3risk_by_analysis_and_adm3.Analysis")
    def test_get_adm3risk_filtered_returns_none_periods_when_analysis_has_no_deforestation(
        self,
        mock_analysis,
        mock_deforestation,
        mock_adm3risk,
    ):
        analysis_oid = ObjectId()
        adm3_oid = ObjectId()

        analysis_doc = SimpleNamespace(id=analysis_oid, deforestation_id=None)
        analysis_qs = MagicMock()
        analysis_qs.no_dereference.return_value = analysis_qs
        analysis_qs.only.return_value = [analysis_doc]
        mock_analysis.objects.return_value = analysis_qs

        coll = MagicMock()
        coll.find.return_value = []
        mock_adm3risk._get_collection.return_value = coll

        data = Adm3RiskFilterRequest(
            analysis_ids=[str(analysis_oid)],
            adm3_ids=[str(adm3_oid)],
        )

        result = get_adm3risk_filtered(data)

        item = result[str(analysis_oid)][0]
        self.assertIsNone(item["period_start"])
        self.assertIsNone(item["period_end"])
        self.assertFalse(item["risk_total"])
        self.assertEqual(item["farm_amount"], 0)
        self.assertEqual(item["farm_total_amount"], 0)
        self.assertEqual(item["def_ha"], 0.0)


if __name__ == "__main__":
    unittest.main()