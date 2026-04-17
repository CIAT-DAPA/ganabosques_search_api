import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bson import DBRef, ObjectId
from fastapi import HTTPException

from src.routes.adm3Front import (
    _as_object_id,
    _split_label,
    _validate_object_ids,
    get_adm3risk_by_adm3_and_type,
    RequestBody,
)


class DummyMongoDoc:
    def __init__(self, mongo_dict):
        self._mongo_dict = mongo_dict

    def to_dict(self):
        return self._mongo_dict


class DummyDeforestation:
    def __init__(self, doc_id, period_start=None, period_end=None):
        self.id = doc_id
        self._period_start = period_start
        self._period_end = period_end

    def to_mongo(self):
        return DummyMongoDoc(
            {
                "_id": self.id,
                "period_start": self._period_start,
                "period_end": self._period_end,
            }
        )


class TestAdm3Front(unittest.TestCase):

    def test_as_object_id_returns_none_for_none(self):
        self.assertIsNone(_as_object_id(None))

    def test_as_object_id_returns_same_objectid_when_input_is_objectid(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id(oid), oid)

    def test_as_object_id_extracts_id_from_dbref(self):
        oid = ObjectId()
        ref = DBRef("collection", oid)

        self.assertEqual(_as_object_id(ref), oid)

    def test_as_object_id_extracts_id_from_dict_with_dollar_id(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id({"$id": oid}), oid)

    def test_as_object_id_returns_none_for_invalid_string(self):
        self.assertIsNone(_as_object_id("not-an-objectid"))

    def test_validate_object_ids_returns_objectids_when_all_are_valid(self):
        raw_ids = [str(ObjectId()), str(ObjectId())]

        result = _validate_object_ids(raw_ids)

        self.assertEqual(len(result), 2)
        self.assertTrue(all(isinstance(item, ObjectId) for item in result))

    def test_validate_object_ids_raises_http_exception_when_any_id_is_invalid(self):
        with self.assertRaises(HTTPException) as context:
            _validate_object_ids([str(ObjectId()), "invalid-id"])

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid ObjectId", context.exception.detail)

    def test_split_label_returns_department_municipality_and_name(self):
        dep, mun, name = _split_label("ANTIOQUIA, MEDELLIN, LA ZONA")

        self.assertEqual(dep, "ANTIOQUIA")
        self.assertEqual(mun, "MEDELLIN")
        self.assertEqual(name, "LA ZONA")

    def test_split_label_returns_none_tuple_when_label_is_missing(self):
        self.assertEqual(_split_label(None), (None, None, None))

    @patch("src.routes.adm3Front.Adm3RiskGroupedResponse")
    @patch("src.routes.adm3Front.Adm3Risk")
    @patch("src.routes.adm3Front.Analysis")
    @patch("src.routes.adm3Front.Deforestation")
    @patch("src.routes.adm3Front.Adm3")
    def test_get_adm3risk_by_adm3_and_type_returns_grouped_response_with_existing_risks(
        self,
        mock_adm3,
        mock_deforestation,
        mock_analysis,
        mock_adm3risk,
        mock_response_class,
    ):
        adm3_id = ObjectId()
        analysis_id = ObjectId()
        deforestation_id = ObjectId()

        adm3_doc = SimpleNamespace(
            id=adm3_id,
            name="LA ZONA",
            label="ANTIOQUIA, MEDELLIN, LA ZONA",
        )

        adm3_queryset = MagicMock()
        adm3_queryset.no_dereference.return_value = adm3_queryset
        adm3_queryset.only.return_value = [adm3_doc]
        mock_adm3.objects.return_value = adm3_queryset

        deforestation_doc = DummyDeforestation(
            deforestation_id,
            period_start=None,
            period_end=None,
        )
        deforestation_queryset = MagicMock()
        deforestation_queryset.no_dereference.return_value = deforestation_queryset
        deforestation_queryset.only.return_value = [deforestation_doc]
        mock_deforestation.objects.return_value = deforestation_queryset

        analysis_doc = SimpleNamespace(
            id=analysis_id,
            deforestation_id=deforestation_id,
            value_chain="cacao",
        )
        analysis_queryset = MagicMock()
        analysis_queryset.filter.return_value = analysis_queryset
        analysis_queryset.no_dereference.return_value = analysis_queryset
        analysis_queryset.only.return_value = [analysis_doc]
        mock_analysis.objects.return_value = analysis_queryset

        mock_collection = MagicMock()
        mock_collection.find.return_value = [
            {
                "adm3_id": adm3_id,
                "analysis_id": analysis_id,
                "risk_total": True,
                "farm_amount": 4,
                "def_ha": 12.5,
            }
        ]
        mock_adm3risk._get_collection.return_value = mock_collection

        mock_response_class.side_effect = lambda root: {"root": root}

        payload = RequestBody(
            adm3_ids=[str(adm3_id)],
            type="annual",
            value_chain=None,
        )

        result = get_adm3risk_by_adm3_and_type(payload)

        self.assertIn("root", result)
        self.assertIn(str(adm3_id), result["root"])
        group = result["root"][str(adm3_id)]
        self.assertEqual(group.adm3_id, str(adm3_id))
        self.assertEqual(group.name, "LA ZONA")
        self.assertEqual(group.department, "ANTIOQUIA")
        self.assertEqual(group.municipality, "MEDELLIN")
        self.assertEqual(len(group.items), 1)
        self.assertTrue(group.items[0].risk_total)
        self.assertEqual(group.items[0].farm_amount, 4)
        self.assertEqual(group.items[0].def_ha, 12.5)

    @patch("src.routes.adm3Front.Adm3RiskGroupedResponse")
    @patch("src.routes.adm3Front.Deforestation")
    @patch("src.routes.adm3Front.Adm3")
    def test_get_adm3risk_by_adm3_and_type_returns_empty_items_when_no_deforestation_periods(
        self,
        mock_adm3,
        mock_deforestation,
        mock_response_class,
    ):
        adm3_id = ObjectId()
        adm3_doc = SimpleNamespace(
            id=adm3_id,
            name="LA ZONA",
            label="ANTIOQUIA, MEDELLIN, LA ZONA",
        )

        adm3_queryset = MagicMock()
        adm3_queryset.no_dereference.return_value = adm3_queryset
        adm3_queryset.only.return_value = [adm3_doc]
        mock_adm3.objects.return_value = adm3_queryset

        deforestation_queryset = MagicMock()
        deforestation_queryset.no_dereference.return_value = deforestation_queryset
        deforestation_queryset.only.return_value = []
        mock_deforestation.objects.return_value = deforestation_queryset

        mock_response_class.side_effect = lambda root: {"root": root}

        payload = RequestBody(
            adm3_ids=[str(adm3_id)],
            type="annual",
            value_chain=None,
        )

        result = get_adm3risk_by_adm3_and_type(payload)

        self.assertIn(str(adm3_id), result["root"])
        self.assertEqual(result["root"][str(adm3_id)].items, [])

    @patch("src.routes.adm3Front.Analysis")
    @patch("src.routes.adm3Front.Deforestation")
    @patch("src.routes.adm3Front.Adm3")
    def test_get_adm3risk_by_adm3_and_type_raises_http_exception_for_invalid_value_chain(
        self,
        mock_adm3,
        mock_deforestation,
        mock_analysis,
    ):
        adm3_id = ObjectId()
        adm3_doc = SimpleNamespace(
            id=adm3_id,
            name="LA ZONA",
            label="ANTIOQUIA, MEDELLIN, LA ZONA",
        )

        adm3_queryset = MagicMock()
        adm3_queryset.no_dereference.return_value = adm3_queryset
        adm3_queryset.only.return_value = [adm3_doc]
        mock_adm3.objects.return_value = adm3_queryset

        deforestation_queryset = MagicMock()
        deforestation_queryset.no_dereference.return_value = deforestation_queryset
        deforestation_queryset.only.return_value = [DummyDeforestation(ObjectId())]
        mock_deforestation.objects.return_value = deforestation_queryset

        mock_analysis.objects.return_value = MagicMock()

        payload = RequestBody(
            adm3_ids=[str(adm3_id)],
            type="annual",
            value_chain="invalid_chain",
        )

        with self.assertRaises(HTTPException) as context:
            get_adm3risk_by_adm3_and_type(payload)

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("value_chain inválido", context.exception.detail)

    def test_get_adm3risk_by_adm3_and_type_raises_http_exception_for_invalid_objectid(self):
        payload = RequestBody(
            adm3_ids=["invalid-id"],
            type="annual",
            value_chain=None,
        )

        with self.assertRaises(HTTPException) as context:
            get_adm3risk_by_adm3_and_type(payload)

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid ObjectId", context.exception.detail)


if __name__ == "__main__":
    unittest.main()