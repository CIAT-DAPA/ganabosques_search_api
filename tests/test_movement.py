import importlib
import sys
import unittest
from collections import defaultdict
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bson import ObjectId
from fastapi import HTTPException

from src.routes.movement import (
    calculate_mixed_python,
    calculate_summary,
    process_movements_python,
    serialize_movement,
)


class TestMovement(unittest.TestCase):

    def _build_classification(self, label="terneros", amount=10):
        return SimpleNamespace(label=label, amount=amount)

    def _build_movement(
        self,
        movement_id="mov-1",
        date=None,
        type_origin="FARM",
        type_destination="ENTERPRISE",
        farm_id_origin=None,
        farm_id_destination=None,
        enterprise_id_origin=None,
        enterprise_id_destination=None,
        species="bovinos",
        classifications=None,
    ):
        return SimpleNamespace(
            id=movement_id,
            date=date,
            type_origin=SimpleNamespace(value=type_origin) if type_origin else None,
            type_destination=SimpleNamespace(value=type_destination) if type_destination else None,
            source_movement=SimpleNamespace(id="source-1"),
            ext_id="EXT-1",
            farm_id_origin=SimpleNamespace(id=farm_id_origin) if farm_id_origin else None,
            farm_id_destination=SimpleNamespace(id=farm_id_destination) if farm_id_destination else None,
            enterprise_id_origin=SimpleNamespace(id=enterprise_id_origin) if enterprise_id_origin else None,
            enterprise_id_destination=SimpleNamespace(id=enterprise_id_destination) if enterprise_id_destination else None,
            movement=classifications or [],
            species=SimpleNamespace(value=species) if species else None,
        )

    def test_serialize_movement_returns_expected_structure(self):
        movement = self._build_movement(
            movement_id="mov-1",
            date=datetime(2024, 6, 1, 0, 0, 0),
            farm_id_origin="farm-1",
            farm_id_destination="farm-2",
            enterprise_id_origin="ent-1",
            enterprise_id_destination="ent-2",
            classifications=[self._build_classification("vacas", 5)],
        )

        result = serialize_movement(movement)

        self.assertEqual(
            result,
            {
                "id": "mov-1",
                "date": "2024-06-01T00:00:00",
                "type_origin": "FARM",
                "type_destination": "ENTERPRISE",
                "source_movement": "source-1",
                "ext_id": "EXT-1",
                "farm_id_origin": "farm-1",
                "farm_id_destination": "farm-2",
                "enterprise_id_origin": "ent-1",
                "enterprise_id_destination": "ent-2",
                "movement": [{"label": "vacas", "amount": 5}],
                "species": "bovinos",
            },
        )

    def test_calculate_summary_returns_expected_percentages(self):
        inputs = {
            "total_movements": 2,
            "movements_by_type": {
                "FARM": 2,
            },
        }
        outputs = {
            "total_movements": 3,
            "movements_by_type": {
                "ENTERPRISE": 3,
            },
        }

        result = calculate_summary(inputs, outputs)

        self.assertEqual(result["total_movements"], 5)
        self.assertEqual(result["inputs"]["count"], 2)
        self.assertEqual(result["outputs"]["count"], 3)
        self.assertEqual(result["inputs"]["percentage"], 40.0)
        self.assertEqual(result["outputs"]["percentage"], 60.0)
        self.assertEqual(result["inputs"]["by_destination_type"]["FARM"]["count"], 2)
        self.assertEqual(result["outputs"]["by_destination_type"]["ENTERPRISE"]["count"], 3)

    def test_calculate_mixed_python_returns_intersections(self):
        inputs_stats = {
            "farms": ["farm-1", "farm-2"],
            "enterprises": ["ent-1", "ent-2"],
        }
        outputs_stats = {
            "farms": ["farm-2", "farm-3"],
            "enterprises": ["ent-2", "ent-3"],
        }

        result = calculate_mixed_python(inputs_stats, outputs_stats)

        self.assertEqual(result["farms"], ["farm-2"])
        self.assertEqual(result["enterprises"], ["ent-2"])

    @patch("src.routes.movement.logger")
    @patch("src.routes.movement.Movement")
    @patch("src.routes.movement.parse_object_ids")
    def test_get_movement_by_farmid_returns_serialized_results(
        self,
        mock_parse_object_ids,
        mock_movement,
        mock_logger,
    ):
        from src.routes.movement import get_movement_by_farmid

        farm_oid = ObjectId()
        movement = self._build_movement(
            movement_id="mov-1",
            date=datetime(2024, 1, 1),
            farm_id_origin=str(farm_oid),
            classifications=[self._build_classification("terneros", 3)],
        )

        mock_parse_object_ids.return_value = [farm_oid]
        queryset = MagicMock()
        queryset.only.return_value.select_related.return_value = [movement]
        mock_movement.objects.filter.return_value = queryset

        result = get_movement_by_farmid(
            ids=str(farm_oid),
            roles=None,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "mov-1")
        self.assertEqual(result[0]["farm_id_origin"], str(farm_oid))

    @patch("src.routes.movement.parse_object_ids")
    def test_get_movement_by_farmid_raises_http_exception_for_invalid_roles(self, mock_parse_object_ids):
        from src.routes.movement import get_movement_by_farmid

        mock_parse_object_ids.return_value = [ObjectId()]

        with self.assertRaises(HTTPException) as context:
            get_movement_by_farmid(
                ids=str(ObjectId()),
                roles="invalid",
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid roles parameter", context.exception.detail)

    @patch("src.routes.movement.parse_object_ids")
    @patch("src.routes.movement.calculate_statistics_python_pure")
    def test_get_movement_statistics_python_pure_returns_grouped_results(
        self,
        mock_calculate_statistics,
        mock_parse_object_ids,
    ):
        from src.routes.movement import get_movement_statistics_python_pure

        farm_oid = ObjectId()
        mock_parse_object_ids.return_value = [str(farm_oid)]
        mock_calculate_statistics.return_value = {"summary": {"total_movements": 1}}

        result = get_movement_statistics_python_pure(
            ids=str(farm_oid),
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        self.assertEqual(result, {str(farm_oid): {"summary": {"total_movements": 1}}})

    def test_get_movement_statistics_python_pure_raises_http_exception_for_invalid_date_range(self):
        from src.routes.movement import get_movement_statistics_python_pure

        with self.assertRaises(HTTPException) as context:
            get_movement_statistics_python_pure(
                ids=str(ObjectId()),
                start_date="2024-12-31",
                end_date="2024-01-01",
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("end_date must be greater than or equal to start_date", context.exception.detail)

    def test_get_movement_statistics_python_pure_raises_http_exception_for_invalid_date_format(self):
        from src.routes.movement import get_movement_statistics_python_pure

        with self.assertRaises(HTTPException) as context:
            get_movement_statistics_python_pure(
                ids=str(ObjectId()),
                start_date="bad-date",
                end_date="2024-12-31",
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid date format", context.exception.detail)

    @patch("src.routes.base_route.generate_read_only_router")
    def test_module_configures_read_only_router_with_expected_arguments(self, mock_generate):
        fake_router = MagicMock(name="inner_router")
        mock_generate.return_value = fake_router

        if "src.routes.movement" in sys.modules:
            del sys.modules["src.routes.movement"]

        module = importlib.import_module("src.routes.movement")

        mock_generate.assert_called_once()
        kwargs = mock_generate.call_args.kwargs

        self.assertEqual(kwargs["prefix"], "/movement")
        self.assertEqual(kwargs["tags"], ["Movement"])
        self.assertEqual(kwargs["collection"], module.Movement)
        self.assertEqual(kwargs["schema_model"], module.MovementSchema)
        self.assertEqual(
            kwargs["allowed_fields"],
            ["ext_id", "species", "type_origin", "type_destination"],
        )
        self.assertEqual(kwargs["serialize_fn"], module.serialize_movement)
        self.assertEqual(kwargs["include_endpoints"], ["paged", "by-extid"])