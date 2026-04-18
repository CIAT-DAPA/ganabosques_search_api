import importlib
import sys
import unittest
from unittest.mock import MagicMock, patch

from fastapi import HTTPException


class TestFarm(unittest.TestCase):

    @patch("src.routes.base_route.generate_read_only_router")
    def test_module_configures_read_only_router_with_expected_arguments(self, mock_generate):
        fake_router = MagicMock(name="inner_router")
        mock_generate.return_value = fake_router

        if "src.routes.farm" in sys.modules:
            del sys.modules["src.routes.farm"]

        module = importlib.import_module("src.routes.farm")

        mock_generate.assert_called_once()
        kwargs = mock_generate.call_args.kwargs

        self.assertEqual(kwargs["prefix"], "/farm")
        self.assertEqual(kwargs["tags"], ["Farm and Enterprise"])
        self.assertEqual(kwargs["collection"], module.Farm)
        self.assertEqual(kwargs["schema_model"], module.FarmSchema)
        self.assertEqual(kwargs["allowed_fields"], ["farm_source"])
        self.assertIsNone(kwargs["serialize_fn"])
        self.assertEqual(kwargs["include_endpoints"], ["paged", "by-extid"])
        self.assertFalse(kwargs["include_get_all"])

    @patch("src.routes.farm.convert_doc_to_json")
    @patch("src.routes.farm.Farm")
    def test_get_all_farms_optimized_returns_converted_items(
        self,
        mock_farm,
        mock_convert_doc_to_json,
    ):
        from src.routes.farm import get_all_farms_optimized

        queryset = MagicMock()
        queryset.as_pymongo.return_value = [{"_id": "1"}, {"_id": "2"}]
        mock_farm.objects.return_value = queryset
        mock_convert_doc_to_json.side_effect = [{"id": "1"}, {"id": "2"}]

        result = get_all_farms_optimized()

        self.assertEqual(result, [{"id": "1"}, {"id": "2"}])
        mock_farm.objects.assert_called_once_with()
        queryset.as_pymongo.assert_called_once()

    @patch("src.routes.farm.Farm")
    def test_get_all_farms_optimized_wraps_unexpected_errors(self, mock_farm):
        from src.routes.farm import get_all_farms_optimized

        mock_farm.objects.side_effect = Exception("db error")

        with self.assertRaises(HTTPException) as context:
            get_all_farms_optimized()

        self.assertEqual(context.exception.status_code, 500)
        self.assertIn("Error retrieving all farms", context.exception.detail)

    @patch("src.routes.farm.convert_doc_to_json")
    @patch("src.routes.farm.Farm")
    @patch("src.routes.farm.parse_object_ids")
    def test_get_farm_by_adm3_ids_returns_converted_items(
        self,
        mock_parse_object_ids,
        mock_farm,
        mock_convert_doc_to_json,
    ):
        from src.routes.farm import get_farm_by_adm3_ids

        mock_parse_object_ids.return_value = ["adm3-1", "adm3-2"]
        queryset = MagicMock()
        queryset.as_pymongo.return_value = [{"_id": "1"}, {"_id": "2"}]
        mock_farm.objects.return_value = queryset
        mock_convert_doc_to_json.side_effect = [{"id": "1"}, {"id": "2"}]

        result = get_farm_by_adm3_ids("adm3-1,adm3-2")

        self.assertEqual(result, [{"id": "1"}, {"id": "2"}])
        mock_parse_object_ids.assert_called_once_with("adm3-1,adm3-2")
        mock_farm.objects.assert_called_once_with(adm3_id__in=["adm3-1", "adm3-2"])

    @patch("src.routes.farm.parse_object_ids")
    def test_get_farm_by_adm3_ids_re_raises_http_exception(self, mock_parse_object_ids):
        from src.routes.farm import get_farm_by_adm3_ids

        mock_parse_object_ids.side_effect = HTTPException(status_code=400, detail="bad ids")

        with self.assertRaises(HTTPException) as context:
            get_farm_by_adm3_ids("bad")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "bad ids")

    @patch("src.routes.farm.Farm")
    @patch("src.routes.farm.parse_object_ids")
    def test_get_farm_by_adm3_ids_wraps_unexpected_errors(
        self,
        mock_parse_object_ids,
        mock_farm,
    ):
        from src.routes.farm import get_farm_by_adm3_ids

        mock_parse_object_ids.return_value = ["adm3-1"]
        mock_farm.objects.side_effect = Exception("db error")

        with self.assertRaises(HTTPException) as context:
            get_farm_by_adm3_ids("adm3-1")

        self.assertEqual(context.exception.status_code, 500)
        self.assertIn("Error retrieving farms by adm3", context.exception.detail)