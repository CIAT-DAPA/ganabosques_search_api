import importlib
import sys
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.routes.suppliers import (
    _build_farm_ext_map_from_suppliers,
    _normalize_years,
    serialize_supplier,
)


class TestSuppliers(unittest.TestCase):

    def test_normalize_years_returns_empty_list_for_none(self):
        self.assertEqual(_normalize_years(None), [])

    def test_normalize_years_normalizes_plain_values(self):
        self.assertEqual(_normalize_years([2017, "2018"]), ["2017", "2018"])

    def test_normalize_years_normalizes_legacy_dict_format(self):
        self.assertEqual(
            _normalize_years([{"years": "2017"}, {"years": 2018}]),
            ["2017", "2018"],
        )

    def test_serialize_supplier_returns_expected_structure(self):
        enterprise_id = "enterprise-1"
        farm_id = "farm-1"

        doc = SimpleNamespace(
            id="supplier-1",
            enterprise_id=SimpleNamespace(id=enterprise_id),
            farm_id=SimpleNamespace(id=farm_id),
            years=[2017, "2018"],
            log=SimpleNamespace(
                enable=True,
                created=datetime(2024, 1, 1, 10, 0, 0),
                updated=datetime(2024, 6, 1, 12, 0, 0),
            ),
        )

        result = serialize_supplier(doc, farm_ext_map={farm_id: [{"ext_code": "S1"}]})

        self.assertEqual(
            result,
            {
                "id": "supplier-1",
                "enterprise_id": enterprise_id,
                "farm_id": farm_id,
                "ext_id": [{"ext_code": "S1"}],
                "years": ["2017", "2018"],
                "log": {
                    "enable": True,
                    "created": "2024-01-01T10:00:00",
                    "updated": "2024-06-01T12:00:00",
                },
            },
        )

    @patch("src.routes.suppliers.Farm")
    def test_build_farm_ext_map_from_suppliers_returns_expected_map(self, mock_farm):
        farm_id = "farm-1"

        supplier = SimpleNamespace(
            farm_id=SimpleNamespace(id=farm_id),
        )

        farm_doc = MagicMock()
        farm_doc.to_mongo.return_value.to_dict.return_value = {
            "_id": farm_id,
            "ext_id": [{"source": "SIT_CODE", "ext_code": "S1"}],
        }

        queryset = MagicMock()
        queryset.no_dereference.return_value = queryset
        queryset.only.return_value = [farm_doc]
        mock_farm.objects.return_value = queryset

        result = _build_farm_ext_map_from_suppliers([supplier])

        self.assertEqual(result, {farm_id: [{"source": "SIT_CODE", "ext_code": "S1"}]})

    @patch("src.routes.suppliers.Farm")
    @patch("src.routes.suppliers.parse_object_ids")
    def test_get_supplier_by_farm_ids_grouped_returns_grouped_results(
        self,
        mock_parse_object_ids,
        mock_farm,
    ):
        from src.routes.suppliers import get_supplier_by_farm_ids_grouped

        farm_id_1 = "farm-1"
        farm_id_2 = "farm-2"

        mock_parse_object_ids.return_value = [farm_id_1, farm_id_2]

        supplier_1 = SimpleNamespace(
            id="sup-1",
            enterprise_id=SimpleNamespace(id="ent-1"),
            farm_id=SimpleNamespace(id=farm_id_1),
            years=[2017],
            log=None,
        )
        supplier_2 = SimpleNamespace(
            id="sup-2",
            enterprise_id=SimpleNamespace(id="ent-2"),
            farm_id=SimpleNamespace(id=farm_id_1),
            years=[2018],
            log=None,
        )

        with patch("src.routes.suppliers.Suppliers") as mock_suppliers:
            mock_suppliers.objects.return_value = [supplier_1, supplier_2]

            farm_doc = MagicMock()
            farm_doc.to_mongo.return_value.to_dict.return_value = {
                "_id": farm_id_1,
                "ext_id": [{"ext_code": "S1"}],
            }
            queryset = MagicMock()
            queryset.no_dereference.return_value = queryset
            queryset.only.return_value = [farm_doc]
            mock_farm.objects.return_value = queryset

            result = get_supplier_by_farm_ids_grouped(f"{farm_id_1},{farm_id_2}")

        self.assertEqual(list(result.keys()), [farm_id_1, farm_id_2])
        self.assertEqual(len(result[farm_id_1]), 2)
        self.assertEqual(result[farm_id_2], [])
        self.assertEqual(result[farm_id_1][0]["ext_id"], [{"ext_code": "S1"}])

    @patch("src.routes.suppliers.Farm")
    @patch("src.routes.suppliers.parse_object_ids")
    def test_get_supplier_by_enterprise_ids_grouped_returns_grouped_results(
        self,
        mock_parse_object_ids,
        mock_farm,
    ):
        from src.routes.suppliers import get_supplier_by_enterprise_ids_grouped

        enterprise_id_1 = "ent-1"
        enterprise_id_2 = "ent-2"
        farm_id = "farm-1"

        mock_parse_object_ids.return_value = [enterprise_id_1, enterprise_id_2]

        supplier = SimpleNamespace(
            id="sup-1",
            enterprise_id=SimpleNamespace(id=enterprise_id_1),
            farm_id=SimpleNamespace(id=farm_id),
            years=[2019],
            log=None,
        )

        with patch("src.routes.suppliers.Suppliers") as mock_suppliers:
            mock_suppliers.objects.return_value = [supplier]

            farm_doc = MagicMock()
            farm_doc.to_mongo.return_value.to_dict.return_value = {
                "_id": farm_id,
                "ext_id": [{"ext_code": "S1"}],
            }
            queryset = MagicMock()
            queryset.no_dereference.return_value = queryset
            queryset.only.return_value = [farm_doc]
            mock_farm.objects.return_value = queryset

            result = get_supplier_by_enterprise_ids_grouped(f"{enterprise_id_1},{enterprise_id_2}")

        self.assertEqual(list(result.keys()), [enterprise_id_1, enterprise_id_2])
        self.assertEqual(len(result[enterprise_id_1]), 1)
        self.assertEqual(result[enterprise_id_2], [])
        self.assertEqual(result[enterprise_id_1][0]["ext_id"], [{"ext_code": "S1"}])

    @patch("src.routes.base_route.generate_read_only_router")
    def test_module_configures_read_only_router_with_expected_arguments(self, mock_generate):
        fake_router = MagicMock(name="inner_router")
        mock_generate.return_value = fake_router

        if "src.routes.suppliers" in sys.modules:
            del sys.modules["src.routes.suppliers"]

        module = importlib.import_module("src.routes.suppliers")

        mock_generate.assert_called_once()
        kwargs = mock_generate.call_args.kwargs

        self.assertEqual(kwargs["prefix"], "/suppliers")
        self.assertEqual(kwargs["tags"], ["Farm and Enterprise"])
        self.assertEqual(kwargs["collection"], module.Suppliers)
        self.assertEqual(kwargs["schema_model"], module.SuppliersSchema)
        self.assertEqual(kwargs["allowed_fields"], [])
        self.assertEqual(kwargs["serialize_fn"], module.serialize_supplier)
        self.assertEqual(kwargs["include_endpoints"], ["paged"])