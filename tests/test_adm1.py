import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from src.routes.adm1 import (
    get_adm1_by_extid,
    get_adm1_by_ids,
    get_adm1_by_name,
    get_adm1_paginated,
    get_all_adm1,
    serialize_adm1,
)


class TestAdm1(unittest.TestCase):

    def _build_adm1(self, doc_id="665f1726b1ac3457e3a91a05", ext_id="5", name="ANTIOQUIA", ugg_size=1.7):
        return SimpleNamespace(
            id=doc_id,
            ext_id=ext_id,
            name=name,
            ugg_size=ugg_size,
        )

    def test_serialize_adm1_returns_expected_dict(self):
        adm = self._build_adm1()

        result = serialize_adm1(adm)

        self.assertEqual(
            result,
            {
                "id": "665f1726b1ac3457e3a91a05",
                "ext_id": "5",
                "name": "ANTIOQUIA",
                "ugg_size": 1.7,
            },
        )

    @patch("src.routes.adm1.Adm1")
    def test_get_all_adm1_returns_serialized_records(self, mock_adm1):
        docs = [
            self._build_adm1(),
            self._build_adm1(
                doc_id="665f1726b1ac3457e3a91a06",
                ext_id="8",
                name="BOLIVAR",
                ugg_size=2.0,
            ),
        ]
        mock_adm1.objects.return_value = docs

        result = get_all_adm1()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "ANTIOQUIA")
        self.assertEqual(result[1]["name"], "BOLIVAR")
        mock_adm1.objects.assert_called_once_with()

    @patch("src.routes.adm1.parse_object_ids")
    @patch("src.routes.adm1.Adm1")
    def test_get_adm1_by_ids_uses_parse_object_ids_and_filters_queryset(self, mock_adm1, mock_parse_object_ids):
        parsed_ids = ["id1", "id2"]
        mock_parse_object_ids.return_value = parsed_ids
        docs = [self._build_adm1()]
        mock_adm1.objects.return_value = docs

        result = get_adm1_by_ids("id1,id2")

        self.assertEqual(len(result), 1)
        mock_parse_object_ids.assert_called_once_with("id1,id2")
        mock_adm1.objects.assert_called_once_with(id__in=parsed_ids)

    @patch("src.routes.adm1.build_search_query")
    @patch("src.routes.adm1.Adm1")
    def test_get_adm1_by_name_builds_query_and_returns_matches(self, mock_adm1, mock_build_search_query):
        mock_build_search_query.return_value = {"$or": []}
        docs = [self._build_adm1(name="ANTIOQUIA")]
        mock_adm1.objects.return_value = docs

        result = get_adm1_by_name(" ant , io ")

        self.assertEqual(len(result), 1)
        mock_build_search_query.assert_called_once_with(["ant", "io"], ["name"])
        mock_adm1.objects.assert_called_once_with(__raw__={"$or": []})

    @patch("src.routes.adm1.build_search_query")
    @patch("src.routes.adm1.Adm1")
    def test_get_adm1_by_extid_builds_query_and_returns_matches(self, mock_adm1, mock_build_search_query):
        mock_build_search_query.return_value = {"$or": []}
        docs = [self._build_adm1(ext_id="5001")]
        mock_adm1.objects.return_value = docs

        result = get_adm1_by_extid("5001,5002")

        self.assertEqual(len(result), 1)
        mock_build_search_query.assert_called_once_with(["5001", "5002"], ["ext_id"])
        mock_adm1.objects.assert_called_once_with(__raw__={"$or": []})

    @patch("src.routes.adm1.build_paginated_response")
    @patch("src.routes.adm1.Adm1")
    def test_get_adm1_paginated_calls_build_paginated_response_with_defaults(
        self,
        mock_adm1,
        mock_build_paginated_response,
    ):
        mock_build_paginated_response.return_value = {"items": [], "total": 0}

        result = get_adm1_paginated(
            page=1,
            limit=10,
            skip=None,
            search=None,
            search_fields=None,
            order_by=None,
        )

        self.assertEqual(result, {"items": [], "total": 0})
        mock_build_paginated_response.assert_called_once()
        kwargs = mock_build_paginated_response.call_args.kwargs
        self.assertEqual(kwargs["base_query"], mock_adm1.objects)
        self.assertEqual(kwargs["page"], 1)
        self.assertEqual(kwargs["limit"], 10)
        self.assertIsNone(kwargs["skip"])
        self.assertEqual(kwargs["order_by_fields"], [])

    @patch("src.routes.adm1.Adm1")
    def test_get_adm1_paginated_raises_http_exception_for_invalid_search_fields(self, mock_adm1):
        with self.assertRaises(HTTPException) as context:
            get_adm1_paginated(
                page=1,
                limit=10,
                skip=None,
                search=None,
                search_fields="name,invalid_field",
                order_by=None,
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid search fields", context.exception.detail)

    @patch("src.routes.adm1.Adm1")
    def test_get_adm1_paginated_raises_http_exception_for_invalid_sort_fields(self, mock_adm1):
        with self.assertRaises(HTTPException) as context:
            get_adm1_paginated(
                page=1,
                limit=10,
                skip=None,
                search=None,
                search_fields=None,
                order_by="name,-invalid_field",
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid sort fields", context.exception.detail)

    @patch("src.routes.adm1.build_search_query")
    @patch("src.routes.adm1.build_paginated_response")
    @patch("src.routes.adm1.Adm1")
    def test_get_adm1_paginated_applies_search_and_sort(
        self,
        mock_adm1,
        mock_build_paginated_response,
        mock_build_search_query,
    ):
        base_query_result = MagicMock(name="filtered_query")
        mock_adm1.objects.return_value = base_query_result
        mock_build_search_query.return_value = {"$or": [{"name": {"$regex": "ant", "$options": "i"}}]}
        mock_build_paginated_response.return_value = {"items": ["ok"]}

        result = get_adm1_paginated(
            page=2,
            limit=5,
            skip=3,
            search="ant,bol",
            search_fields="name,ext_id",
            order_by="name,-ext_id",
        )

        self.assertEqual(result, {"items": ["ok"]})
        mock_build_search_query.assert_called_once_with(["ant", "bol"], ["name", "ext_id"])
        mock_adm1.objects.assert_called_once_with(__raw__={"$or": [{"name": {"$regex": "ant", "$options": "i"}}]})
        kwargs = mock_build_paginated_response.call_args.kwargs
        self.assertEqual(kwargs["base_query"], base_query_result)
        self.assertEqual(kwargs["page"], 2)
        self.assertEqual(kwargs["limit"], 5)
        self.assertEqual(kwargs["skip"], 3)
        self.assertEqual(kwargs["order_by_fields"], ["name", "-ext_id"])


if __name__ == "__main__":
    unittest.main()