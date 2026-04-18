import importlib
import sys
import unittest
from unittest.mock import MagicMock, patch

from fastapi import HTTPException


class TestEnterprise(unittest.TestCase):

    @patch("src.routes.base_route.generate_read_only_router")
    def test_module_configures_read_only_router_with_expected_arguments(self, mock_generate):
        fake_router = MagicMock(name="inner_router")
        mock_generate.return_value = fake_router

        if "src.routes.enterprise" in sys.modules:
            del sys.modules["src.routes.enterprise"]

        module = importlib.import_module("src.routes.enterprise")

        mock_generate.assert_called_once()
        kwargs = mock_generate.call_args.kwargs

        self.assertEqual(kwargs["prefix"], "/enterprise")
        self.assertEqual(kwargs["tags"], ["Farm and Enterprise"])
        self.assertEqual(kwargs["collection"], module.Enterprise)
        self.assertEqual(kwargs["schema_model"], module.EnterpriseSchema)
        self.assertEqual(kwargs["allowed_fields"], ["name", "type_enterprise"])
        self.assertIsNone(kwargs["serialize_fn"])
        self.assertEqual(kwargs["include_endpoints"], ["paged", "by-name", "by-extid"])

    @patch("src.routes.enterprise.convert_doc_to_json")
    @patch("src.routes.enterprise.Enterprise")
    @patch("src.routes.enterprise.parse_object_ids")
    def test_get_enterprise_by_adm2_ids_returns_converted_items(
        self,
        mock_parse_object_ids,
        mock_enterprise,
        mock_convert_doc_to_json,
    ):
        from src.routes.enterprise import get_enterprise_by_adm2_ids

        mock_parse_object_ids.return_value = ["adm2-1", "adm2-2"]
        queryset = MagicMock()
        queryset.as_pymongo.return_value = [
            {"_id": "1", "name": "E1"},
            {"_id": "2", "name": "E2"},
        ]
        mock_enterprise.objects.return_value = queryset
        mock_convert_doc_to_json.side_effect = [
            {"id": "1", "name": "E1"},
            {"id": "2", "name": "E2"},
        ]

        result = get_enterprise_by_adm2_ids("adm2-1,adm2-2")

        self.assertEqual(
            result,
            [
                {"id": "1", "name": "E1"},
                {"id": "2", "name": "E2"},
            ],
        )
        mock_parse_object_ids.assert_called_once_with("adm2-1,adm2-2")
        mock_enterprise.objects.assert_called_once_with(adm2_id__in=["adm2-1", "adm2-2"])
        queryset.as_pymongo.assert_called_once()

    @patch("src.routes.enterprise.Enterprise")
    @patch("src.routes.enterprise.parse_object_ids")
    def test_get_enterprise_by_adm2_ids_re_raises_http_exception(
        self,
        mock_parse_object_ids,
        mock_enterprise,
    ):
        from src.routes.enterprise import get_enterprise_by_adm2_ids

        mock_parse_object_ids.side_effect = HTTPException(status_code=400, detail="bad ids")

        with self.assertRaises(HTTPException) as context:
            get_enterprise_by_adm2_ids("bad")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "bad ids")

    @patch("src.routes.enterprise.Enterprise")
    @patch("src.routes.enterprise.parse_object_ids")
    def test_get_enterprise_by_adm2_ids_wraps_unexpected_errors(
        self,
        mock_parse_object_ids,
        mock_enterprise,
    ):
        from src.routes.enterprise import get_enterprise_by_adm2_ids

        mock_parse_object_ids.return_value = ["adm2-1"]
        mock_enterprise.objects.side_effect = Exception("db failure")

        with self.assertRaises(HTTPException) as context:
            get_enterprise_by_adm2_ids("adm2-1")

        self.assertEqual(context.exception.status_code, 500)
        self.assertIn("Error retrieving enterprises by adm2", context.exception.detail)