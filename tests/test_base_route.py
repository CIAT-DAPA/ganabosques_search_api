import unittest
from unittest.mock import MagicMock, patch

from fastapi import HTTPException
from pydantic import BaseModel
from typing import Optional

from ganabosques_orm.enums.valuechain import ValueChain
from src.routes.base_route import generate_read_only_router


class TestBaseRoute(unittest.TestCase):

    def _get_endpoint(self, router, path, method):
        for route in router.routes:
            methods = getattr(route, "methods", set())
            if route.path == path and method in methods:
                return route.endpoint
        self.fail(f"No se encontró endpoint {method} {path}")

    def _build_collection(self, name="FakeCollection"):
        return type(
            name,
            (),
            {
                "objects": MagicMock(),
            },
        )

    def test_generate_read_only_router_creates_router_with_prefix_and_tags(self):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name"],
            include_endpoints=["paged"],
        )

        self.assertEqual(router.prefix, "/fake")
        paths = {route.path for route in router.routes}
        self.assertIn("/fake/", paths)
        self.assertIn("/fake/by-ids", paths)
        self.assertIn("/fake/paged/", paths)

    @patch("src.routes.base_route.convert_doc_to_json")
    def test_get_all_uses_default_serializer_when_serialize_fn_is_none(self, mock_convert_doc_to_json):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        doc = MagicMock()
        doc.to_mongo.return_value.to_dict.return_value = {"_id": "1"}
        collection.objects.return_value = [doc]

        mock_convert_doc_to_json.return_value = {"id": "1"}

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name"],
            serialize_fn=None,
        )

        endpoint = self._get_endpoint(router, "/fake/", "GET")
        result = endpoint()

        self.assertEqual(result, [{"id": "1"}])
        mock_convert_doc_to_json.assert_called_once_with({"_id": "1"})

    def test_get_all_uses_custom_serializer_when_serialize_fn_is_provided(self):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        doc = MagicMock()
        collection.objects.return_value = [doc]

        serialize_fn = MagicMock(return_value={"id": "custom"})

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name"],
            serialize_fn=serialize_fn,
        )

        endpoint = self._get_endpoint(router, "/fake/", "GET")
        result = endpoint()

        self.assertEqual(result, [{"id": "custom"}])
        serialize_fn.assert_called_once_with(doc)

    def test_get_all_raises_http_exception_when_collection_fails(self):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        collection.objects.side_effect = Exception("db error")

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name"],
        )

        endpoint = self._get_endpoint(router, "/fake/", "GET")

        with self.assertRaises(HTTPException) as context:
            endpoint()

        self.assertEqual(context.exception.status_code, 500)
        self.assertIn("Error retrieving Fake Collection records", context.exception.detail)

    @patch("src.routes.base_route.parse_object_ids")
    def test_get_by_ids_uses_parse_object_ids_and_serializes_matches(self, mock_parse_object_ids):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        doc = MagicMock()
        doc.to_mongo.return_value.to_dict.return_value = {"_id": "1"}
        mock_parse_object_ids.return_value = ["id1", "id2"]
        collection.objects.return_value = [doc]

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name"],
        )

        endpoint = self._get_endpoint(router, "/fake/by-ids", "GET")

        with patch("src.routes.base_route.convert_doc_to_json", return_value={"id": "1"}):
            result = endpoint(ids="id1,id2")

        self.assertEqual(result, [{"id": "1"}])
        mock_parse_object_ids.assert_called_once_with("id1,id2")
        collection.objects.assert_called_once_with(id__in=["id1", "id2"])

    @patch("src.routes.base_route.parse_object_ids")
    def test_get_by_ids_re_raises_http_exception(self, mock_parse_object_ids):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        mock_parse_object_ids.side_effect = HTTPException(status_code=400, detail="bad ids")

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name"],
        )

        endpoint = self._get_endpoint(router, "/fake/by-ids", "GET")

        with self.assertRaises(HTTPException) as context:
            endpoint(ids="bad")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "bad ids")

    @patch("src.routes.base_route.build_search_query")
    def test_by_name_endpoint_is_created_and_filters_by_name(self, mock_build_search_query):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str
            name: Optional[str] = None

        mock_build_search_query.return_value = {"$or": []}
        doc = MagicMock()
        doc.to_mongo.return_value.to_dict.return_value = {"_id": "1"}
        collection.objects.return_value = [doc]

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name"],
            include_endpoints=["by-name"],
        )

        endpoint = self._get_endpoint(router, "/fake/by-name", "GET")

        with patch("src.routes.base_route.convert_doc_to_json", return_value={"id": "1"}):
            result = endpoint(name="uno,dos", value_chain=None)

        self.assertEqual(result, [{"id": "1"}])
        mock_build_search_query.assert_called_once_with(["uno", "dos"], ["name"])
        collection.objects.assert_called_once_with(__raw__={"$or": []})

    @patch("src.routes.base_route.build_search_query")
    def test_by_name_applies_value_chain_filter_when_provided(self, mock_build_search_query):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str
            name: Optional[str] = None

        mock_build_search_query.return_value = {"$or": []}
        collection.objects.return_value = []

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name"],
            include_endpoints=["by-name"],
        )

        endpoint = self._get_endpoint(router, "/fake/by-name", "GET")
        value_chain = list(ValueChain)[0]

        endpoint(name="uno", value_chain=value_chain)

        collection.objects.assert_called_once_with(
            __raw__={"$or": []},
            value_chain=value_chain.value,
        )

    def test_by_name_endpoint_is_not_created_when_not_included(self):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name"],
            include_endpoints=["paged"],
        )

        paths = {route.path for route in router.routes}
        self.assertNotIn("/fake/by-name", paths)

    @patch("src.routes.base_route.build_search_query")
    def test_by_extid_simple_variant_uses_build_search_query(self, mock_build_search_query):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str
            ext_id: Optional[str] = None

        mock_build_search_query.return_value = {"$or": []}
        doc = MagicMock()
        doc.to_mongo.return_value.to_dict.return_value = {"_id": "1"}
        collection.objects.return_value = [doc]

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name"],
            include_endpoints=["by-extid"],
        )

        endpoint = self._get_endpoint(router, "/fake/by-extid", "GET")

        with patch("src.routes.base_route.convert_doc_to_json", return_value={"id": "1"}):
            result = endpoint(ext_ids="A1,A2")

        self.assertEqual(result, [{"id": "1"}])
        mock_build_search_query.assert_called_once_with(["A1", "A2"], ["ext_id"])
        collection.objects.assert_called_once_with(__raw__={"$or": []})

    @patch("src.routes.base_route.build_paginated_response")
    def test_get_paginated_calls_build_paginated_response_with_defaults(self, mock_build_paginated_response):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        mock_build_paginated_response.return_value = {"items": [], "total": 0}

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name", "ext_id"],
            include_endpoints=["paged"],
        )

        endpoint = self._get_endpoint(router, "/fake/paged/", "GET")

        result = endpoint(
            page=1,
            limit=10,
            skip=None,
            search=None,
            search_fields=None,
            order_by=None,
        )

        self.assertEqual(result, {"items": [], "total": 0})
        kwargs = mock_build_paginated_response.call_args.kwargs
        self.assertEqual(kwargs["base_query"], collection.objects)
        self.assertEqual(kwargs["page"], 1)
        self.assertEqual(kwargs["limit"], 10)
        self.assertIsNone(kwargs["skip"])
        self.assertEqual(kwargs["order_by_fields"], [])

    @patch("src.routes.base_route.build_search_query")
    @patch("src.routes.base_route.build_paginated_response")
    def test_get_paginated_applies_search_and_sort(self, mock_build_paginated_response, mock_build_search_query):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        filtered_query = MagicMock(name="filtered_query")
        collection.objects.return_value = filtered_query
        mock_build_search_query.return_value = {"$or": []}
        mock_build_paginated_response.return_value = {"items": ["ok"]}

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name", "ext_id"],
            include_endpoints=["paged"],
        )

        endpoint = self._get_endpoint(router, "/fake/paged/", "GET")

        result = endpoint(
            page=2,
            limit=5,
            skip=3,
            search="uno,dos",
            search_fields="name,ext_id",
            order_by="name,-ext_id",
        )

        self.assertEqual(result, {"items": ["ok"]})
        mock_build_search_query.assert_called_once_with(["uno", "dos"], ["name", "ext_id"])

        kwargs = mock_build_paginated_response.call_args.kwargs
        self.assertEqual(kwargs["base_query"], filtered_query)
        self.assertEqual(kwargs["page"], 2)
        self.assertEqual(kwargs["limit"], 5)
        self.assertEqual(kwargs["skip"], 3)
        self.assertEqual(kwargs["order_by_fields"], ["name", "-ext_id"])

    @patch("src.routes.base_route.build_paginated_response")
    def test_get_paginated_raises_http_exception_when_search_is_too_broad(self, mock_build_paginated_response):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11"],
            include_endpoints=["paged"],
        )

        endpoint = self._get_endpoint(router, "/fake/paged/", "GET")

        with self.assertRaises(HTTPException) as context:
            endpoint(
                page=1,
                limit=10,
                skip=None,
                search="a,b,c,d,e",
                search_fields="f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11",
                order_by=None,
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Search is too broad", context.exception.detail)

    @patch("src.routes.base_route.build_paginated_response")
    def test_get_paginated_raises_http_exception_for_invalid_sort_field(self, mock_build_paginated_response):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        router = generate_read_only_router(
            prefix="/fake",
            tags=["Fake"],
            collection=collection,
            schema_model=FakeSchema,
            allowed_fields=["name", "ext_id"],
            include_endpoints=["paged"],
        )

        endpoint = self._get_endpoint(router, "/fake/paged/", "GET")

        with self.assertRaises(HTTPException) as context:
            endpoint(
                page=1,
                limit=10,
                skip=None,
                search=None,
                search_fields=None,
                order_by="name,-bad_field",
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid sort field: bad_field", context.exception.detail)

    def test_get_paginated_uses_only_allowed_search_fields(self):
        collection = self._build_collection()

        class FakeSchema(BaseModel):
            id: str

        with patch("src.routes.base_route.build_search_query", return_value={"$or": []}) as mock_build_search_query:
            with patch("src.routes.base_route.build_paginated_response", return_value={"items": []}):
                filtered_query = MagicMock(name="filtered_query")
                collection.objects.return_value = filtered_query

                router = generate_read_only_router(
                    prefix="/fake",
                    tags=["Fake"],
                    collection=collection,
                    schema_model=FakeSchema,
                    allowed_fields=["name", "ext_id"],
                    include_endpoints=["paged"],
                )

                endpoint = self._get_endpoint(router, "/fake/paged/", "GET")

                endpoint(
                    page=1,
                    limit=10,
                    skip=None,
                    search="uno",
                    search_fields="name,invalid,ext_id",
                    order_by=None,
                )

                mock_build_search_query.assert_called_once_with(["uno"], ["name", "ext_id"])


if __name__ == "__main__":
    unittest.main()