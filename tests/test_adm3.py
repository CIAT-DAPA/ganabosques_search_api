import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from src.routes.adm3 import (
    get_adm3_by_adm2_ids,
    get_adm3_by_extid,
    get_adm3_by_ids,
    get_adm3_by_label,
    get_adm3_by_name,
    get_adm3_paginated,
    get_all_adm3,
    serialize_adm3,
)


class TestAdm3(unittest.TestCase):

    def _build_adm2(self, doc_id="adm2-id", name="MEDELLIN"):
        return SimpleNamespace(id=doc_id, name=name)

    def _build_adm3(self, doc_id="adm3-id", ext_id="7001", name="LA ZONA", adm2=None, label="ANTIOQUIA, MEDELLIN, LA ZONA"):
        return SimpleNamespace(
            id=doc_id,
            ext_id=ext_id,
            name=name,
            adm2_id=adm2,
            label=label,
        )

    def test_serialize_adm3_returns_expected_dict(self):
        adm2 = self._build_adm2()
        adm3 = self._build_adm3(adm2=adm2)

        result = serialize_adm3(adm3)

        self.assertEqual(
            result,
            {
                "id": "adm3-id",
                "ext_id": "7001",
                "name": "LA ZONA",
                "adm2_id": "adm2-id",
                "adm2_name": "MEDELLIN",
                "label": "ANTIOQUIA, MEDELLIN, LA ZONA",
            },
        )

    @patch("src.routes.adm3.Adm3")
    def test_get_all_adm3_limits_to_1000_and_serializes_results(self, mock_adm3):
        docs = [self._build_adm3()]
        limited_docs = MagicMock()
        limited_docs.__iter__.return_value = iter(docs)

        queryset = MagicMock()
        queryset.limit.return_value = limited_docs
        mock_adm3.objects.return_value = queryset

        result = get_all_adm3()

        self.assertEqual(len(result), 1)
        queryset.limit.assert_called_once_with(1000)

    @patch("src.routes.adm3.Adm3")
    def test_get_adm3_by_ids_raises_http_exception_when_any_id_is_invalid(self, mock_adm3):
        with self.assertRaises(HTTPException) as context:
            get_adm3_by_ids("507f1f77bcf86cd799439011,invalid_id")

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("IDs no válidos", context.exception.detail)

    @patch("src.routes.adm3.Adm3")
    def test_get_adm3_by_ids_filters_queryset_when_ids_are_valid(self, mock_adm3):
        valid_id_1 = "507f1f77bcf86cd799439011"
        valid_id_2 = "507f191e810c19729de860ea"
        mock_adm3.objects.return_value = [self._build_adm3()]

        result = get_adm3_by_ids(f"{valid_id_1},{valid_id_2}")

        self.assertEqual(len(result), 1)
        mock_adm3.objects.assert_called_once_with(id__in=[valid_id_1, valid_id_2])

    @patch("src.routes.adm3.Adm3")
    def test_get_adm3_by_name_builds_raw_regex_query(self, mock_adm3):
        mock_adm3.objects.return_value = [self._build_adm3()]

        result = get_adm3_by_name("charco azul,las palmas")

        self.assertEqual(len(result), 1)
        mock_adm3.objects.assert_called_once_with(
            __raw__={
                "$or": [
                    {"name": {"$regex": "charco azul", "$options": "i"}},
                    {"name": {"$regex": "las palmas", "$options": "i"}},
                ]
            }
        )

    @patch("src.routes.adm3.build_search_query")
    @patch("src.routes.adm3.Adm3")
    def test_get_adm3_by_extid_uses_build_search_query(self, mock_adm3, mock_build_search_query):
        mock_build_search_query.return_value = {"$or": []}
        mock_adm3.objects.return_value = [self._build_adm3()]

        result = get_adm3_by_extid("7001,7002")

        self.assertEqual(len(result), 1)
        mock_build_search_query.assert_called_once_with(["7001", "7002"], ["ext_id"])
        mock_adm3.objects.assert_called_once_with(__raw__={"$or": []})

    @patch("src.routes.adm3.parse_object_ids")
    @patch("src.routes.adm3.Adm3")
    def test_get_adm3_by_adm2_ids_uses_parse_object_ids(self, mock_adm3, mock_parse_object_ids):
        mock_parse_object_ids.return_value = ["adm2-1"]
        mock_adm3.objects.return_value = [self._build_adm3()]

        result = get_adm3_by_adm2_ids("adm2-1")

        self.assertEqual(len(result), 1)
        mock_adm3.objects.assert_called_once_with(adm2_id__in=["adm2-1"])

    @patch("src.routes.adm3.Adm3")
    def test_get_adm3_by_label_uses_icontains_lookup(self, mock_adm3):
        mock_adm3.objects.return_value = [self._build_adm3()]

        result = get_adm3_by_label("MEDELLIN")

        self.assertEqual(len(result), 1)
        mock_adm3.objects.assert_called_once_with(label__icontains="MEDELLIN")

    @patch("src.routes.adm3.Adm3")
    def test_get_adm3_paginated_raises_http_exception_for_invalid_search_fields(self, mock_adm3):
        with self.assertRaises(HTTPException) as context:
            get_adm3_paginated(
                page=1,
                limit=10,
                skip=None,
                search=None,
                search_fields="name,bad",
                order_by=None,
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid search fields", context.exception.detail)

    @patch("src.routes.adm3.Adm3")
    def test_get_adm3_paginated_raises_http_exception_for_invalid_sort_fields(self, mock_adm3):
        with self.assertRaises(HTTPException) as context:
            get_adm3_paginated(
                page=1,
                limit=10,
                skip=None,
                search=None,
                search_fields=None,
                order_by="name,-bad",
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid sort fields", context.exception.detail)

    @patch("src.routes.adm3.build_search_query")
    @patch("src.routes.adm3.build_paginated_response")
    @patch("src.routes.adm3.Adm3")
    def test_get_adm3_paginated_applies_filters_and_calls_pagination(
        self,
        mock_adm3,
        mock_build_paginated_response,
        mock_build_search_query,
    ):
        filtered_query = MagicMock(name="filtered_query")
        mock_adm3.objects.return_value = filtered_query
        mock_build_search_query.return_value = {"$or": [{"name": {"$regex": "zona", "$options": "i"}}]}
        mock_build_paginated_response.return_value = {"items": []}

        result = get_adm3_paginated(
            page=2,
            limit=15,
            skip=5,
            search="zona,la",
            search_fields="name,ext_id",
            order_by="name,-ext_id",
        )

        self.assertEqual(result, {"items": []})
        mock_build_search_query.assert_called_once_with(["zona", "la"], ["name", "ext_id"])
        mock_adm3.objects.assert_called_once_with(__raw__={"$or": [{"name": {"$regex": "zona", "$options": "i"}}]})

        kwargs = mock_build_paginated_response.call_args.kwargs
        self.assertEqual(kwargs["base_query"], filtered_query)
        self.assertEqual(kwargs["page"], 2)
        self.assertEqual(kwargs["limit"], 15)
        self.assertEqual(kwargs["skip"], 5)
        self.assertEqual(kwargs["order_by_fields"], ["name", "-ext_id"])
        self.assertEqual(kwargs["serialize_fn"], serialize_adm3)


if __name__ == "__main__":
    unittest.main()