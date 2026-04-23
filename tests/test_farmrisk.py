import importlib
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from src.routes.farmrisk import _as_str_oid, _serialize_attr, serialize_farmrisk


class TestFarmRisk(unittest.TestCase):

    def test_as_str_oid_returns_none_for_none(self):
        self.assertIsNone(_as_str_oid(None))

    def test_as_str_oid_returns_string_for_plain_value(self):
        self.assertEqual(_as_str_oid("abc"), "abc")

    def test_as_str_oid_returns_string_for_object_with_id_attribute(self):
        self.assertEqual(_as_str_oid(SimpleNamespace(id="obj-id")), "obj-id")

    def test_serialize_attr_returns_none_when_missing(self):
        self.assertIsNone(_serialize_attr(None))

    def test_serialize_attr_returns_expected_dict(self):
        attr = SimpleNamespace(prop=0.2, ha=1.4, distance=30.5)

        result = _serialize_attr(attr)

        self.assertEqual(result, {"prop": 0.2, "ha": 1.4})

    def test_serialize_farmrisk_returns_expected_structure(self):
        doc = SimpleNamespace(
            id="risk-1",
            farm_id=SimpleNamespace(id="farm-1"),
            analysis_id=SimpleNamespace(id="analysis-1"),
            farm_polygons_id=SimpleNamespace(id="polygon-1"),
            deforestation=SimpleNamespace(prop=0.1, ha=2.0),
            protected=SimpleNamespace(prop=0.2, ha=1.0),
            farming_in=SimpleNamespace(prop=0.3, ha=3.0),
            farming_out=SimpleNamespace(prop=0.4, ha=4.0),
            risk_direct=True,
            risk_input=False,
            risk_output=True,
        )

        result = serialize_farmrisk(doc)

        self.assertEqual(
            result,
            {
                "id": "risk-1",
                "farm_id": "farm-1",
                "analysis_id": "analysis-1",
                "farm_polygons_id": "polygon-1",
                "deforestation": {"prop": 0.1, "ha": 2.0},
                "protected": {"prop": 0.2, "ha": 1.0},
                "farming_in": {"prop": 0.3, "ha": 3.0},
                "farming_out": {"prop": 0.4, "ha": 4.0},
                "risk_direct": True,
                "risk_input": False,
                "risk_output": True,
            },
        )

    @patch("src.routes.base_route.generate_read_only_router")
    def test_module_configures_read_only_router_with_expected_arguments(self, mock_generate):
        fake_router = MagicMock(name="inner_router")
        mock_generate.return_value = fake_router

        if "src.routes.farmrisk" in sys.modules:
            del sys.modules["src.routes.farmrisk"]

        module = importlib.import_module("src.routes.farmrisk")

        mock_generate.assert_called_once()
        kwargs = mock_generate.call_args.kwargs

        self.assertEqual(kwargs["prefix"], "/farmrisk")
        self.assertEqual(kwargs["tags"], ["Analysis risk"])
        self.assertEqual(kwargs["collection"], module.FarmRisk)
        self.assertEqual(kwargs["schema_model"], module.FarmRiskSchema)
        self.assertEqual(
            kwargs["allowed_fields"],
            ["farm_id", "analysis_id", "risk_direct", "risk_input", "risk_output"],
        )
        self.assertEqual(kwargs["serialize_fn"], module.serialize_farmrisk)
        self.assertEqual(kwargs["include_endpoints"], ["paged"])

    @patch("src.routes.farmrisk.convert_doc_to_json")
    @patch("src.routes.farmrisk.FarmRisk")
    @patch("src.routes.farmrisk.parse_object_ids")
    def test_get_farmrisk_by_analysis_ids_returns_converted_items(
        self,
        mock_parse_object_ids,
        mock_farmrisk,
        mock_convert_doc_to_json,
    ):
        from src.routes.farmrisk import get_farmrisk_by_analysis_ids

        mock_parse_object_ids.return_value = ["analysis-1", "analysis-2"]
        queryset = MagicMock()
        queryset.as_pymongo.return_value = [{"_id": "1"}, {"_id": "2"}]
        mock_farmrisk.objects.return_value = queryset
        mock_convert_doc_to_json.side_effect = [{"id": "1"}, {"id": "2"}]

        result = get_farmrisk_by_analysis_ids("analysis-1,analysis-2")

        self.assertEqual(result, [{"id": "1"}, {"id": "2"}])
        mock_parse_object_ids.assert_called_once_with("analysis-1,analysis-2")
        mock_farmrisk.objects.assert_called_once_with(analysis_id__in=["analysis-1", "analysis-2"])

    @patch("src.routes.farmrisk.parse_object_ids")
    def test_get_farmrisk_by_analysis_ids_re_raises_http_exception(self, mock_parse_object_ids):
        from src.routes.farmrisk import get_farmrisk_by_analysis_ids

        mock_parse_object_ids.side_effect = HTTPException(status_code=400, detail="bad ids")

        with self.assertRaises(HTTPException) as context:
            get_farmrisk_by_analysis_ids("bad")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "bad ids")

    @patch("src.routes.farmrisk.FarmRisk")
    @patch("src.routes.farmrisk.parse_object_ids")
    def test_get_farmrisk_by_analysis_ids_wraps_unexpected_errors(
        self,
        mock_parse_object_ids,
        mock_farmrisk,
    ):
        from src.routes.farmrisk import get_farmrisk_by_analysis_ids

        mock_parse_object_ids.return_value = ["analysis-1"]
        mock_farmrisk.objects.side_effect = Exception("db error")

        with self.assertRaises(HTTPException) as context:
            get_farmrisk_by_analysis_ids("analysis-1")

        self.assertEqual(context.exception.status_code, 500)
        self.assertIn("Error retrieving farmrisk by analysis ids", context.exception.detail)