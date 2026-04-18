import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bson import DBRef, ObjectId
from fastapi import HTTPException

from src.routes.adm3risk_get_all import (
    _area,
    _as_object_id,
    _extract_sit_codes_from_farm_ext_id,
    _get_periods_and_analyses,
    #_safe_iso if False else None,  # placeholder to avoid lint in some editors
    _iso,
    _split_label_3,
    _to_oid_list,
    _uniq,
    _validate_object_ids,
    GlobalRequest,
    get_risk_by_ids_and_type,
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


class TestAdm3RiskGetAll(unittest.TestCase):

    def test_as_object_id_returns_none_for_none(self):
        self.assertIsNone(_as_object_id(None))

    def test_as_object_id_returns_objectid_from_objectid(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id(oid), oid)

    def test_as_object_id_returns_objectid_from_dbref(self):
        oid = ObjectId()
        ref = DBRef("collection", oid)
        self.assertEqual(_as_object_id(ref), oid)

    def test_as_object_id_returns_objectid_from_dict_with_dollar_id(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id({"$id": oid}), oid)

    def test_as_object_id_returns_none_for_invalid_string(self):
        self.assertIsNone(_as_object_id("invalid"))

    def test_validate_object_ids_returns_valid_objectids(self):
        ids = [str(ObjectId()), str(ObjectId())]

        result = _validate_object_ids(ids)

        self.assertEqual(len(result), 2)
        self.assertTrue(all(isinstance(x, ObjectId) for x in result))

    def test_validate_object_ids_raises_http_exception_for_invalid_id(self):
        with self.assertRaises(HTTPException) as context:
            _validate_object_ids([str(ObjectId()), "bad-id"])

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid ObjectId", context.exception.detail)

    def test_split_label_3_returns_department_municipality_and_vereda(self):
        result = _split_label_3("ANTIOQUIA, MEDELLIN, LA ZONA")
        self.assertEqual(result, ("ANTIOQUIA", "MEDELLIN", "LA ZONA"))

    def test_split_label_3_returns_nones_when_label_missing(self):
        self.assertEqual(_split_label_3(None), (None, None, None))

    def test_area_returns_default_values_when_input_is_not_dict(self):
        self.assertEqual(_area(None), {"ha": 0.0, "prop": 0.0})

    def test_area_returns_float_values_from_dict(self):
        self.assertEqual(_area({"ha": 10, "prop": 0.25}), {"ha": 10.0, "prop": 0.25})

    def test_to_oid_list_returns_empty_list_for_none(self):
        self.assertEqual(_to_oid_list(None), [])

    def test_to_oid_list_converts_mixed_list_to_valid_objectids(self):
        oid = ObjectId()
        result = _to_oid_list([oid, str(ObjectId()), "bad-id"])
        self.assertEqual(len(result), 2)
        self.assertTrue(all(isinstance(x, ObjectId) for x in result))

    def test_extract_sit_codes_from_farm_ext_id_returns_only_sit_code_values(self):
        ext_id = [
            {"source": "OTHER", "ext_code": "111"},
            {"source": "SIT_CODE", "ext_code": "ABC"},
            {"source": "SIT_CODE", "ext_code": 123},
        ]

        result = _extract_sit_codes_from_farm_ext_id(ext_id)

        self.assertEqual(result, ["ABC", "123"])

    def test_uniq_returns_unique_values_preserving_order(self):
        self.assertEqual(_uniq(["A", "B", "A", "C", "B"]), ["A", "B", "C"])

    @patch("src.routes.adm3risk_get_all.Analysis")
    @patch("src.routes.adm3risk_get_all.Deforestation")
    def test_get_periods_and_analyses_from_analysis_ids(self, mock_deforestation, mock_analysis):
        analysis_id = ObjectId()
        defo_id = ObjectId()

        analysis_doc = SimpleNamespace(id=analysis_id, deforestation_id=defo_id)
        analysis_qs = MagicMock()
        analysis_qs.no_dereference.return_value = analysis_qs
        analysis_qs.only.return_value = [analysis_doc]
        mock_analysis.objects.return_value = analysis_qs

        defo_doc = DummyDeforestation(defo_id)
        defo_qs = MagicMock()
        defo_qs.no_dereference.return_value = defo_qs
        defo_qs.only.return_value = [defo_doc]
        mock_deforestation.objects.return_value = defo_qs

        payload = GlobalRequest(
            entity_type="adm3",
            ids=[str(ObjectId())],
            analysis_ids=[str(analysis_id)],
        )

        defo_periods, analysis_to_defo = _get_periods_and_analyses(payload)

        self.assertIn(str(defo_id), defo_periods)
        self.assertEqual(analysis_to_defo[str(analysis_id)], str(defo_id))

    @patch("src.routes.adm3risk_get_all.Analysis")
    @patch("src.routes.adm3risk_get_all.Deforestation")
    def test_get_periods_and_analyses_from_type(self, mock_deforestation, mock_analysis):
        defo_id = ObjectId()
        analysis_id = ObjectId()

        defo_doc = DummyDeforestation(defo_id)
        defo_qs = MagicMock()
        defo_qs.no_dereference.return_value = defo_qs
        defo_qs.only.return_value = [defo_doc]
        mock_deforestation.objects.return_value = defo_qs

        analysis_doc = SimpleNamespace(id=analysis_id, deforestation_id=defo_id)
        analysis_qs = MagicMock()
        analysis_qs.no_dereference.return_value = analysis_qs
        analysis_qs.only.return_value = [analysis_doc]
        mock_analysis.objects.return_value = analysis_qs

        payload = GlobalRequest(
            entity_type="adm3",
            ids=[str(ObjectId())],
            type="annual",
        )

        defo_periods, analysis_to_defo = _get_periods_and_analyses(payload)

        self.assertIn(str(defo_id), defo_periods)
        self.assertEqual(analysis_to_defo[str(analysis_id)], str(defo_id))

    def test_get_periods_and_analyses_raises_http_exception_when_no_mode_is_provided(self):
        payload = GlobalRequest(
            entity_type="adm3",
            ids=[str(ObjectId())],
        )

        with self.assertRaises(HTTPException) as context:
            _get_periods_and_analyses(payload)

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Must provide either type OR analysis_ids OR deforestation_ids", context.exception.detail)

    @patch("src.routes.adm3risk_get_all._build_adm3_sit_codes_for_analysis")
    @patch("src.routes.adm3risk_get_all.Farm")
    @patch("src.routes.adm3risk_get_all.Adm3Risk")
    @patch("src.routes.adm3risk_get_all._get_periods_and_analyses")
    @patch("src.routes.adm3risk_get_all.Adm3")
    def test_get_risk_by_ids_and_type_returns_grouped_adm3_result(
        self,
        mock_adm3,
        mock_get_periods,
        mock_adm3risk,
        mock_farm,
        mock_build_sit_codes,
    ):
        adm3_id = ObjectId()
        analysis_id = str(ObjectId())
        defo_id = str(ObjectId())

        adm3_doc = SimpleNamespace(
            id=adm3_id,
            name="LA ZONA",
            label="ANTIOQUIA, MEDELLIN, LA ZONA",
        )
        adm3_qs = MagicMock()
        adm3_qs.no_dereference.return_value = adm3_qs
        adm3_qs.only.return_value = [adm3_doc]
        mock_adm3.objects.return_value = adm3_qs

        mock_get_periods.return_value = ({defo_id: (None, None)}, {analysis_id: defo_id})

        adm3risk_coll = MagicMock()
        adm3risk_coll.find.return_value = [
            {
                "analysis_id": ObjectId(analysis_id),
                "adm3_id": adm3_id,
                "risk_total": True,
                "farm_amount": 5,
                "def_ha": 20.5,
            }
        ]
        mock_adm3risk._get_collection.return_value = adm3risk_coll

        farm_qs = MagicMock()
        farm_qs.no_dereference.return_value = farm_qs
        farm_qs.only.return_value = []
        mock_farm.objects.return_value = farm_qs

        mock_build_sit_codes.return_value = {
            str(adm3_id): {
                "direct": ["S1"],
                "input": ["S2"],
                "output": ["S3"],
            }
        }

        payload = GlobalRequest(
            entity_type="adm3",
            ids=[str(adm3_id)],
            type="annual",
        )

        result = get_risk_by_ids_and_type(payload)

        self.assertIn(str(adm3_id), result)
        self.assertEqual(result[str(adm3_id)]["name"], "LA ZONA")
        self.assertEqual(len(result[str(adm3_id)]["items"]), 1)
        self.assertTrue(result[str(adm3_id)]["items"][0]["risk_total"])
        self.assertEqual(result[str(adm3_id)]["items"][0]["farm_amount"], 5)
        self.assertEqual(result[str(adm3_id)]["items"][0]["def_ha"], 20.5)
        self.assertEqual(
            result[str(adm3_id)]["items"][0]["sit_codes"],
            {"direct": ["S1"], "input": ["S2"], "output": ["S3"]},
        )

    @patch("src.routes.adm3risk_get_all.Adm3")
    @patch("src.routes.adm3risk_get_all._get_periods_and_analyses")
    def test_get_risk_by_ids_and_type_returns_empty_adm3_items_when_no_periods(
        self,
        mock_get_periods,
        mock_adm3,
    ):
        adm3_id = ObjectId()

        adm3_doc = SimpleNamespace(
            id=adm3_id,
            name="LA ZONA",
            label="ANTIOQUIA, MEDELLIN, LA ZONA",
        )
        adm3_qs = MagicMock()
        adm3_qs.no_dereference.return_value = adm3_qs
        adm3_qs.only.return_value = [adm3_doc]
        mock_adm3.objects.return_value = adm3_qs

        mock_get_periods.return_value = ({}, {})

        payload = GlobalRequest(
            entity_type="adm3",
            ids=[str(adm3_id)],
            type="annual",
        )

        result = get_risk_by_ids_and_type(payload)

        self.assertIn(str(adm3_id), result)
        self.assertEqual(result[str(adm3_id)]["items"], [])

    @patch("src.routes.adm3risk_get_all.Adm3")
    @patch("src.routes.adm3risk_get_all.Farm")
    @patch("src.routes.adm3risk_get_all.FarmRisk")
    @patch("src.routes.adm3risk_get_all._get_periods_and_analyses")
    def test_get_risk_by_ids_and_type_returns_grouped_farm_result(
        self,
        mock_get_periods,
        mock_farmrisk,
        mock_farm,
        mock_adm3,
    ):
        farm_id = ObjectId()
        adm3_id = ObjectId()
        analysis_id = str(ObjectId())
        defo_id = str(ObjectId())

        farm_doc = MagicMock()
        farm_doc.to_mongo.return_value.to_dict.return_value = {
            "_id": farm_id,
            "adm3_id": adm3_id,
            "ext_id": [{"source": "SIT_CODE", "ext_code": "A1"}],
            "log": {"created": None, "updated": None},
        }
        farm_qs = MagicMock()
        farm_qs.no_dereference.return_value = farm_qs
        farm_qs.only.return_value = [farm_doc]
        mock_farm.objects.return_value = farm_qs

        adm3_doc = SimpleNamespace(id=adm3_id, label="ANTIOQUIA, MEDELLIN, VEREDA X")
        adm3_qs = MagicMock()
        adm3_qs.no_dereference.return_value = adm3_qs
        adm3_qs.only.return_value = [adm3_doc]
        mock_adm3.objects.return_value = adm3_qs

        mock_get_periods.return_value = ({defo_id: (None, None)}, {analysis_id: defo_id})

        farmrisk_coll = MagicMock()
        farmrisk_coll.find.return_value = [
            {
                "analysis_id": ObjectId(analysis_id),
                "farm_id": farm_id,
                "risk_direct": True,
                "risk_input": False,
                "risk_output": True,
                "deforestation": {"ha": 1.5, "prop": 0.1},
                "farming_in": {"ha": 0.0, "prop": 0.0},
                "farming_out": {"ha": 2.0, "prop": 0.2},
                "protected": {"ha": 0.5, "prop": 0.05},
            }
        ]
        mock_farmrisk._get_collection.return_value = farmrisk_coll

        payload = GlobalRequest(
            entity_type="farm",
            ids=[str(farm_id)],
            type="annual",
        )

        result = get_risk_by_ids_and_type(payload)

        self.assertIn(str(farm_id), result)
        self.assertEqual(result[str(farm_id)]["farm"]["department"], "ANTIOQUIA")
        self.assertEqual(result[str(farm_id)]["farm"]["municipality"], "MEDELLIN")
        self.assertEqual(result[str(farm_id)]["farm"]["vereda"], "VEREDA X")
        self.assertEqual(len(result[str(farm_id)]["items"]), 1)
        self.assertTrue(result[str(farm_id)]["items"][0]["risk_direct"])
        self.assertEqual(
            result[str(farm_id)]["items"][0]["deforestation"],
            {"ha": 1.5, "prop": 0.1},
        )

    @patch("src.routes.adm3risk_get_all.Farm")
    @patch("src.routes.adm3risk_get_all.Enterprise")
    @patch("src.routes.adm3risk_get_all.EnterpriseRisk")
    @patch("src.routes.adm3risk_get_all.FarmRisk")
    @patch("src.routes.adm3risk_get_all.Adm2")
    @patch("src.routes.adm3risk_get_all.Adm1")
    @patch("src.routes.adm3risk_get_all._get_periods_and_analyses")
    def test_get_risk_by_ids_and_type_returns_grouped_enterprise_result(
        self,
        mock_get_periods,
        mock_adm1,
        mock_adm2,
        mock_farmrisk,
        mock_enterpriserisk,
        mock_enterprise,
        mock_farm,
    ):
        enterprise_id = ObjectId()
        adm2_id = ObjectId()
        adm1_id = ObjectId()
        analysis_id = str(ObjectId())
        defo_id = str(ObjectId())
        farmrisk_id = ObjectId()
        farm_id = ObjectId()

        enterprise_coll = MagicMock()
        enterprise_coll.find.return_value = [
            {
                "_id": enterprise_id,
                "adm2_id": adm2_id,
                "name": "EMPRESA X",
                "ext_id": "EXT1",
                "type_enterprise": "coop",
                "latitude": 1.1,
                "longitud": -2.2,
                "log": {},
            }
        ]
        mock_enterprise._get_collection.return_value = enterprise_coll

        adm2_doc = MagicMock()
        adm2_doc.to_mongo.return_value.to_dict.return_value = {
            "_id": adm2_id,
            "name": "MEDELLIN",
            "adm1_id": adm1_id,
        }
        adm2_qs = MagicMock()
        adm2_qs.no_dereference.return_value = adm2_qs
        adm2_qs.only.return_value = [adm2_doc]
        mock_adm2.objects.return_value = adm2_qs

        adm1_doc = SimpleNamespace(id=adm1_id, name="ANTIOQUIA")
        adm1_qs = MagicMock()
        adm1_qs.no_dereference.return_value = adm1_qs
        adm1_qs.only.return_value = [adm1_doc]
        mock_adm1.objects.return_value = adm1_qs

        mock_get_periods.return_value = ({defo_id: (None, None)}, {analysis_id: defo_id})

        er_coll = MagicMock()
        er_coll.find.return_value = [
            {
                "enterprise_id": enterprise_id,
                "analysis_id": ObjectId(analysis_id),
                "risk_input": [farmrisk_id],
                "risk_output": [farmrisk_id],
            }
        ]
        mock_enterpriserisk._get_collection.return_value = er_coll

        fr_coll = MagicMock()
        fr_coll.find.return_value = [{"_id": farmrisk_id, "farm_id": farm_id}]
        mock_farmrisk._get_collection.return_value = fr_coll

        farm_doc = MagicMock()
        farm_doc.to_mongo.return_value.to_dict.return_value = {
            "_id": farm_id,
            "ext_id": [{"source": "SIT_CODE", "ext_code": "SC1"}],
        }
        farm_qs = MagicMock()
        farm_qs.no_dereference.return_value = farm_qs
        farm_qs.only.return_value = [farm_doc]
        mock_farm.objects.return_value = farm_qs

        payload = GlobalRequest(
            entity_type="enterprise",
            ids=[str(enterprise_id)],
            type="annual",
        )

        result = get_risk_by_ids_and_type(payload)

        self.assertIn(str(enterprise_id), result)
        enterprise = result[str(enterprise_id)]["enterprise"]
        self.assertEqual(enterprise["department"], "ANTIOQUIA")
        self.assertEqual(enterprise["municipality"], "MEDELLIN")
        self.assertEqual(len(result[str(enterprise_id)]["items"]), 1)
        self.assertEqual(
            result[str(enterprise_id)]["items"][0]["sit_codes"],
            {"input": ["SC1"], "output": ["SC1"]},
        )

    def test_get_risk_by_ids_and_type_raises_http_exception_for_invalid_entity_type(self):
        payload = GlobalRequest(
            entity_type="adm3",
            ids=["invalid-id"],
            type="annual",
        )

        with self.assertRaises(HTTPException) as context:
            get_risk_by_ids_and_type(payload)

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid ObjectId", context.exception.detail)


if __name__ == "__main__":
    unittest.main()