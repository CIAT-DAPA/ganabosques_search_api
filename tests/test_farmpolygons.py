import importlib
import sys
import unittest
from unittest.mock import MagicMock, patch

from fastapi import HTTPException


class TestFarmPolygons(unittest.TestCase):

    @patch("src.routes.base_route.generate_read_only_router")
    def test_module_configures_read_only_router_with_expected_arguments(self, mock_generate):
        fake_router = MagicMock(name="inner_router")
        mock_generate.return_value = fake_router

        if "src.routes.farmpolygons" in sys.modules:
            del sys.modules["src.routes.farmpolygons"]

        module = importlib.import_module("src.routes.farmpolygons")

        mock_generate.assert_called_once()
        kwargs = mock_generate.call_args.kwargs

        self.assertEqual(kwargs["prefix"], "/farmpolygons")
        self.assertEqual(kwargs["tags"], ["Farm and Enterprise"])
        self.assertEqual(kwargs["collection"], module.FarmPolygons)
        self.assertEqual(kwargs["schema_model"], module.FarmPolygonsSchema)
        self.assertEqual(kwargs["allowed_fields"], [])
        self.assertIsNone(kwargs["serialize_fn"])
        self.assertEqual(kwargs["include_endpoints"], ["paged"])
        self.assertFalse(kwargs["include_get_all"])

    @patch("src.routes.farmpolygons.convert_doc_to_json")
    @patch("src.routes.farmpolygons.FarmPolygons")
    def test_get_all_farmpolygons_optimized_returns_converted_items(
        self,
        mock_farmpolygons,
        mock_convert_doc_to_json,
    ):
        from src.routes.farmpolygons import get_all_farmpolygons_optimized

        queryset = MagicMock()
        queryset.as_pymongo.return_value = [
            {"_id": "1", "geojson": "{}"},
            {"_id": "2", "geojson": "{}"},
        ]
        mock_farmpolygons.objects.return_value = queryset
        mock_convert_doc_to_json.side_effect = [
            {"id": "1", "geojson": "{}"},
            {"id": "2", "geojson": "{}"},
        ]

        result = get_all_farmpolygons_optimized()

        self.assertEqual(result, [{"id": "1", "geojson": "{}"}, {"id": "2", "geojson": "{}"}])
        mock_farmpolygons.objects.assert_called_once_with()
        queryset.as_pymongo.assert_called_once()

    @patch("src.routes.farmpolygons.FarmPolygons")
    def test_get_all_farmpolygons_optimized_wraps_unexpected_errors(self, mock_farmpolygons):
        from src.routes.farmpolygons import get_all_farmpolygons_optimized

        mock_farmpolygons.objects.side_effect = Exception("db error")

        with self.assertRaises(HTTPException) as context:
            get_all_farmpolygons_optimized()

        self.assertEqual(context.exception.status_code, 500)
        self.assertIn("Error retrieving all farmpolygons", context.exception.detail)

    @patch("src.routes.farmpolygons.convert_doc_to_json")
    @patch("src.routes.farmpolygons.FarmPolygons")
    @patch("src.routes.farmpolygons.parse_object_ids")
    def test_get_farmpolygons_by_farm_ids_returns_converted_items(
        self,
        mock_parse_object_ids,
        mock_farmpolygons,
        mock_convert_doc_to_json,
    ):
        from src.routes.farmpolygons import get_farmpolygons_by_farm_ids

        mock_parse_object_ids.return_value = ["farm-1", "farm-2"]
        queryset = MagicMock()
        queryset.as_pymongo.return_value = [{"_id": "1"}, {"_id": "2"}]
        mock_farmpolygons.objects.return_value = queryset
        mock_convert_doc_to_json.side_effect = [{"id": "1"}, {"id": "2"}]

        result = get_farmpolygons_by_farm_ids("farm-1,farm-2")

        self.assertEqual(result, [{"id": "1"}, {"id": "2"}])
        mock_parse_object_ids.assert_called_once_with("farm-1,farm-2")
        mock_farmpolygons.objects.assert_called_once_with(farm_id__in=["farm-1", "farm-2"])

    @patch("src.routes.farmpolygons.parse_object_ids")
    def test_get_farmpolygons_by_farm_ids_re_raises_http_exception(self, mock_parse_object_ids):
        from src.routes.farmpolygons import get_farmpolygons_by_farm_ids

        mock_parse_object_ids.side_effect = HTTPException(status_code=400, detail="bad ids")

        with self.assertRaises(HTTPException) as context:
            get_farmpolygons_by_farm_ids("bad")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "bad ids")

    @patch("src.routes.farmpolygons.FarmPolygons")
    @patch("src.routes.farmpolygons.parse_object_ids")
    def test_get_farmpolygons_by_farm_ids_wraps_unexpected_errors(
        self,
        mock_parse_object_ids,
        mock_farmpolygons,
    ):
        from src.routes.farmpolygons import get_farmpolygons_by_farm_ids

        mock_parse_object_ids.return_value = ["farm-1"]
        mock_farmpolygons.objects.side_effect = Exception("db error")

        with self.assertRaises(HTTPException) as context:
            get_farmpolygons_by_farm_ids("farm-1")

        self.assertEqual(context.exception.status_code, 500)
        self.assertIn("Error retrieving farmpolygons by farm ids", context.exception.detail)