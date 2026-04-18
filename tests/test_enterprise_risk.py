import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bson import DBRef, ObjectId
from fastapi import HTTPException

from src.routes.enterprise_risk import (
    MAX_IDS,
    Request,
    _as_object_id,
    _build_providers_from_er_list,
    _doc_to_dict,
    _stringify,
    _validate_oids,
    get_enterprise_risk_grouped_by_enterprise,
)


class DummyMongoWrapper:
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return self._data


class DummyDoc:
    def __init__(self, data):
        self._data = data
        self.id = data.get("_id")

    def to_mongo(self):
        return DummyMongoWrapper(self._data)


class TestEnterpriseRisk(unittest.TestCase):

    def test_as_object_id_returns_none_for_none(self):
        self.assertIsNone(_as_object_id(None))

    def test_as_object_id_returns_same_objectid(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id(oid), oid)

    def test_as_object_id_returns_dbref_id(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id(DBRef("x", oid)), oid)

    def test_as_object_id_supports_dict_with_dollar_id(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id({"$id": str(oid)}), oid)

    def test_as_object_id_supports_dict_with_dollar_oid(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id({"$oid": str(oid)}), oid)

    def test_as_object_id_returns_none_for_invalid_value(self):
        self.assertIsNone(_as_object_id("bad-id"))

    def test_validate_oids_returns_unique_valid_objectids(self):
        oid1 = str(ObjectId())
        oid2 = str(ObjectId())

        result = _validate_oids([oid1, oid1, oid2], "enterprise_ids")

        self.assertEqual(result, [ObjectId(oid1), ObjectId(oid2)])

    def test_validate_oids_raises_when_exceeding_max_ids(self):
        ids = [str(ObjectId()) for _ in range(MAX_IDS + 1)]

        with self.assertRaises(HTTPException) as context:
            _validate_oids(ids, "enterprise_ids")

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("excede", context.exception.detail)

    def test_validate_oids_raises_for_invalid_id(self):
        with self.assertRaises(HTTPException) as context:
            _validate_oids(["bad-id"], "enterprise_ids")

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid enterprise_ids", context.exception.detail)

    def test_stringify_converts_objectids_dbrefs_and_nested_values(self):
        oid = ObjectId()
        dt = SimpleNamespace()
        import datetime as _dt

        value = {
            "a": oid,
            "b": DBRef("c", oid),
            "c": [_dt.datetime(2024, 1, 1, 0, 0, 0)],
        }

        result = _stringify(value)

        self.assertEqual(result["a"], str(oid))
        self.assertEqual(result["b"], str(oid))
        self.assertEqual(result["c"][0], "2024-01-01T00:00:00")

    def test_doc_to_dict_uses_stringify_on_mongo_dict(self):
        oid = ObjectId()
        doc = DummyDoc({"_id": oid})

        result = _doc_to_dict(doc)

        self.assertEqual(result, {"_id": str(oid)})

    def test_build_providers_from_er_list_returns_inputs_and_outputs(self):
        farm_id = ObjectId()
        fr_id = ObjectId()

        ers = [
            {
                "risk_input": [str(fr_id)],
                "risk_output": [str(fr_id)],
            }
        ]
        fr_by_id = {
            str(fr_id): {
                "_id": str(fr_id),
                "farm_id": str(farm_id),
                "risk_direct": True,
            }
        }
        farm_by_id = {
            str(farm_id): {
                "_id": str(farm_id),
                "name": "Farm 1",
            }
        }

        result = _build_providers_from_er_list(ers, fr_by_id, farm_by_id)

        self.assertEqual(len(result["inputs"]), 1)
        self.assertEqual(len(result["outputs"]), 1)
        self.assertEqual(result["inputs"][0]["_id"], str(farm_id))
        self.assertEqual(result["inputs"][0]["risk"]["_id"], str(fr_id))

    def test_get_enterprise_risk_grouped_by_enterprise_raises_for_invalid_analysis_id(self):
        with self.assertRaises(HTTPException) as context:
            get_enterprise_risk_grouped_by_enterprise(
                Request(analysis_id="bad-id", enterprise_ids=[])
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "analysis_id inválido")

    @patch("src.routes.enterprise_risk.Deforestation")
    @patch("src.routes.enterprise_risk.Analysis")
    @patch("src.routes.enterprise_risk.Adm1")
    @patch("src.routes.enterprise_risk.Adm2")
    @patch("src.routes.enterprise_risk.Enterprise")
    @patch("src.routes.enterprise_risk.Farm")
    @patch("src.routes.enterprise_risk.FarmRisk")
    @patch("src.routes.enterprise_risk.EnterpriseRisk")
    def test_get_enterprise_risk_grouped_by_enterprise_returns_expected_structure(
        self,
        mock_enterpriserisk,
        mock_farmrisk,
        mock_farm,
        mock_enterprise,
        mock_adm2,
        mock_adm1,
        mock_analysis,
        mock_deforestation,
    ):
        analysis_oid = ObjectId()
        enterprise_oid = ObjectId()
        adm2_oid = ObjectId()
        adm1_oid = ObjectId()
        farmrisk_oid = ObjectId()
        farm_oid = ObjectId()
        deforestation_oid = ObjectId()

        current_er_doc = DummyDoc(
            {
                "_id": ObjectId(),
                "enterprise_id": enterprise_oid,
                "analysis_id": analysis_oid,
                "risk_input": [farmrisk_oid],
                "risk_output": [farmrisk_oid],
            }
        )

        history_er_doc = DummyDoc(
            {
                "_id": ObjectId(),
                "enterprise_id": enterprise_oid,
                "analysis_id": analysis_oid,
                "risk_input": [farmrisk_oid],
                "risk_output": [],
            }
        )

        current_qs = MagicMock()
        current_qs.no_dereference.return_value = current_qs
        current_qs.only.return_value = [current_er_doc]

        history_qs = MagicMock()
        history_qs.no_dereference.return_value = history_qs
        history_qs.only.return_value = [history_er_doc]

        def enterprise_risk_objects_side_effect(**kwargs):
            if kwargs.get("analysis_id") == analysis_oid:
                return current_qs
            return history_qs

        mock_enterpriserisk.objects.side_effect = enterprise_risk_objects_side_effect

        fr_doc = DummyDoc(
            {
                "_id": farmrisk_oid,
                "farm_id": farm_oid,
                "analysis_id": analysis_oid,
                "risk_direct": True,
            }
        )
        fr_qs = MagicMock()
        fr_qs.no_dereference.return_value = fr_qs
        fr_qs.only.return_value = [fr_doc]
        mock_farmrisk.objects.return_value = fr_qs

        farm_doc = DummyDoc(
            {
                "_id": farm_oid,
                "adm3_id": ObjectId(),
                "ext_id": [],
                "farm_source": "source",
            }
        )
        farm_qs = MagicMock()
        farm_qs.no_dereference.return_value = farm_qs
        farm_qs.only.return_value = [farm_doc]
        mock_farm.objects.return_value = farm_qs

        enterprise_doc = DummyDoc(
            {
                "_id": enterprise_oid,
                "adm2_id": adm2_oid,
                "name": "Enterprise 1",
                "ext_id": [],
                "type_enterprise": "COLLECTION_CENTER",
                "latitude": 1.2,
                "longitud": -2.3,
            }
        )
        enterprise_qs = MagicMock()
        enterprise_qs.no_dereference.return_value = enterprise_qs
        enterprise_qs.only.return_value = [enterprise_doc]
        mock_enterprise.objects.return_value = enterprise_qs

        adm2_doc = DummyDoc(
            {
                "_id": adm2_oid,
                "name": "Municipality",
                "adm1_id": adm1_oid,
            }
        )
        adm2_qs = MagicMock()
        adm2_qs.no_dereference.return_value = adm2_qs
        adm2_qs.only.return_value = [adm2_doc]
        mock_adm2.objects.return_value = adm2_qs

        adm1_doc = DummyDoc(
            {
                "_id": adm1_oid,
                "name": "Department",
            }
        )
        adm1_qs = MagicMock()
        adm1_qs.no_dereference.return_value = adm1_qs
        adm1_qs.only.return_value = [adm1_doc]
        mock_adm1.objects.return_value = adm1_qs

        analysis_doc = DummyDoc(
            {
                "_id": analysis_oid,
                "deforestation_id": deforestation_oid,
            }
        )
        analysis_qs = MagicMock()
        analysis_qs.no_dereference.return_value = analysis_qs
        analysis_qs.only.return_value = [analysis_doc]
        mock_analysis.objects.return_value = analysis_qs

        deforestation_doc = DummyDoc(
            {
                "_id": deforestation_oid,
                "deforestation_type": "annual",
                "period_start": "2024-01-01T00:00:00",
                "period_end": "2024-12-31T23:59:59",
            }
        )
        deforestation_qs = MagicMock()
        deforestation_qs.no_dereference.return_value = deforestation_qs
        deforestation_qs.only.return_value = [deforestation_doc]
        mock_deforestation.objects.return_value = deforestation_qs

        result = get_enterprise_risk_grouped_by_enterprise(
            Request(analysis_id=str(analysis_oid), enterprise_ids=[str(enterprise_oid)])
        )

        self.assertEqual(len(result), 1)
        item = result[0]
        self.assertEqual(item["_id"], str(enterprise_oid))
        self.assertEqual(item["name"], "Enterprise 1")
        self.assertEqual(item["adm2"]["name"], "Municipality")
        self.assertEqual(item["adm1"]["name"], "Department")
        self.assertIn("providers", item)
        self.assertIn("history", item)
        self.assertEqual(len(item["providers"]["inputs"]), 1)
        self.assertEqual(len(item["history"]["annual"]), 1)
        self.assertEqual(item["history"]["cumulative"], [])