import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from src.routes.adm2 import (
    get_adm2_by_adm1_ids,
    get_adm2_by_ids,
    get_adm2_by_name,
    get_adm1_by_extid,
    get_adm2_paginated,
    get_all_adm2,
    serialize_adm2,
)


class TestAdm2(unittest.TestCase):

    def _build_adm1(self, doc_id="adm1-id", name="ANTIOQUIA"):
        return SimpleNamespace(id=doc_id, name=name)

    def _build_adm2(self, doc_id="adm2-id", ext_id="5001", name="MEDELLIN", adm1=None):
        return SimpleNamespace(
            id=doc_id,
            ext_id=ext_id,
            name=name,
            adm1_id=adm1,
        )

    def test_serialize_adm2_returns_expected_dict_with_adm1(self):
        adm1 = self._build_adm1()
        adm2 = self._build_adm2(adm1=adm1)

        result = serialize_adm2(adm2)

        self.assertEqual(
            result,
            {
                "id": "adm2-id",
                "ext_id": "5001",
                "name": "MEDELLIN",
                "adm1_id": "adm1-id",
                "adm1_name": "ANTIOQUIA",
            },
        )

    def test_serialize_adm2_returns_none_fields_when_adm1_is_missing(self):
        adm2 = self._build_adm2(adm1=None)

        result = serialize_adm2(adm2)

        self.assertIsNone(result["adm1_id"])
        self.assertIsNone(result["adm1_name"])

    @patch("src.routes.adm2.Adm2")
    def test_get_all_adm2_limits_to_1000_and_serializes_results(self, mock_adm2):
        docs = [self._build_adm2()]
        limited_docs = MagicMock()
        limited_docs.__iter__.return_value = iter(docs)

        queryset = MagicMock()
        queryset.limit.return_value = limited_docs
        mock_adm2.objects.return_value = queryset

        result = get_all_adm2()

        self.assertEqual(len(result), 1)
        queryset.limit.assert_called_once_with(1000)

    @patch("src.routes.adm2.parse_object_ids")
    @patch("src.routes.adm2.Adm2")
    def test_get_adm2_by_ids_filters_by_parsed_ids(self, mock_adm2, mock_parse_object_ids):
        mock_parse_object_ids.return_value = ["id1", "id2"]
        mock_adm2.objects.return_value = [self._build_adm2()]

        result = get_adm2_by_ids("id1,id2")

        self.assertEqual(len(result), 1)
        mock_adm2.objects.assert_called_once_with(id__in=["id1", "id2"])

    @patch("src.routes.adm2.build_search_query")
    @patch("src.routes.adm2.Adm2")
    def test_get_adm2_by_name_uses_build_search_query(self, mock_adm2, mock_build_search_query):
        mock_build_search_query.return_value = {"$or": []}
        mock_adm2.objects.return_value = [self._build_adm2()]

        result = get_adm2_by_name("Cali,Palmira")

        self.assertEqual(len(result), 1)
        mock_build_search_query.assert_called_once_with(["Cali", "Palmira"], ["name"])
        mock_adm2.objects.assert_called_once_with(__raw__={"$or": []})

    @patch("src.routes.adm2.build_search_query")
    @patch("src.routes.adm2.Adm2")
    def test_get_adm1_by_extid_uses_build_search_query(self, mock_adm2, mock_build_search_query):
        mock_build_search_query.return_value = {"$or": []}
        mock_adm2.objects.return_value = [self._build_adm2()]

        result = get_adm1_by_extid("5001,5002")

        self.assertEqual(len(result), 1)
        mock_build_search_query.assert_called_once_with(["5001", "5002"], ["ext_id"])
        mock_adm2.objects.assert_called_once_with(__raw__={"$or": []})

    @patch("src.routes.adm2.parse_object_ids")
    @patch("src.routes.adm2.Adm2")
    def test_get_adm2_by_adm1_ids_filters_by_adm1_ids(self, mock_adm2, mock_parse_object_ids):
        mock_parse_object_ids.return_value = ["adm1-1", "adm1-2"]
        mock_adm2.objects.return_value = [self._build_adm2()]

        result = get_adm2_by_adm1_ids("adm1-1,adm1-2")

        self.assertEqual(len(result), 1)
        mock_adm2.objects.assert_called_once_with(adm1_id__in=["adm1-1", "adm1-2"])

    @patch("src.routes.adm2.Adm2")
    def test_get_adm2_paginated_raises_http_exception_for_invalid_search_fields(self, mock_adm2):
        with self.assertRaises(HTTPException) as context:
            get_adm2_paginated(
                page=1,
                limit=10,
                skip=None,
                search=None,
                search_fields="name,bad_field",
                order_by=None,
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid search fields", context.exception.detail)

    @patch("src.routes.adm2.Adm2")
    def test_get_adm2_paginated_raises_http_exception_for_invalid_sort_fields(self, mock_adm2):
        with self.assertRaises(HTTPException) as context:
            get_adm2_paginated(
                page=1,
                limit=10,
                skip=None,
                search=None,
                search_fields=None,
                order_by="name,-bad_field",
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid sort fields", context.exception.detail)

    @patch("src.routes.adm2.build_search_query")
    @patch("src.routes.adm2.build_paginated_response")
    @patch("src.routes.adm2.Adm2")
    def test_get_adm2_paginated_applies_filters_and_calls_pagination(
        self,
        mock_adm2,
        mock_build_paginated_response,
        mock_build_search_query,
    ):
        filtered_query = MagicMock(name="filtered_query")
        mock_adm2.objects.return_value = filtered_query
        mock_build_search_query.return_value = {"$or": [{"name": {"$regex": "med", "$options": "i"}}]}
        mock_build_paginated_response.return_value = {"items": []}

        result = get_adm2_paginated(
            page=3,
            limit=20,
            skip=40,
            search="med,cali",
            search_fields="name,ext_id",
            order_by="name,-ext_id",
        )

        self.assertEqual(result, {"items": []})
        mock_build_search_query.assert_called_once_with(["med", "cali"], ["name", "ext_id"])
        mock_adm2.objects.assert_called_once_with(__raw__={"$or": [{"name": {"$regex": "med", "$options": "i"}}]})

        kwargs = mock_build_paginated_response.call_args.kwargs
        self.assertEqual(kwargs["base_query"], filtered_query)
        self.assertEqual(kwargs["page"], 3)
        self.assertEqual(kwargs["limit"], 20)
        self.assertEqual(kwargs["skip"], 40)
        self.assertEqual(kwargs["order_by_fields"], ["name", "-ext_id"])
        self.assertEqual(kwargs["serialize_fn"], serialize_adm2)


if __name__ == "__main__":
    unittest.main()