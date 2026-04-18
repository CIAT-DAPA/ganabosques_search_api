import importlib
import sys
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.routes.farmingareas import serialize_farming_areas


class TestFarmingAreas(unittest.TestCase):

    def test_serialize_farming_areas_returns_expected_structure(self):
        doc = SimpleNamespace(
            id="fa-1",
            name="Zona Agricola",
            path="/geo/farming/zona.geojson",
            log=SimpleNamespace(
                enable=True,
                created=datetime(2022, 8, 1, 12, 0, 0),
                updated=datetime(2024, 6, 10, 15, 30, 0),
            ),
        )

        result = serialize_farming_areas(doc)

        self.assertEqual(
            result,
            {
                "id": "fa-1",
                "name": "Zona Agricola",
                "path": "/geo/farming/zona.geojson",
                "log": {
                    "enable": True,
                    "created": "2022-08-01T12:00:00",
                    "updated": "2024-06-10T15:30:00",
                },
            },
        )

    def test_serialize_farming_areas_returns_none_log_when_missing(self):
        doc = SimpleNamespace(
            id="fa-1",
            name="Zona Agricola",
            path="/geo/farming/zona.geojson",
            log=None,
        )

        result = serialize_farming_areas(doc)

        self.assertEqual(
            result,
            {
                "id": "fa-1",
                "name": "Zona Agricola",
                "path": "/geo/farming/zona.geojson",
                "log": None,
            },
        )

    @patch("src.routes.base_route.generate_read_only_router")
    def test_module_configures_read_only_router_with_expected_arguments(self, mock_generate):
        fake_router = MagicMock(name="inner_router")
        mock_generate.return_value = fake_router

        if "src.routes.farmingareas" in sys.modules:
            del sys.modules["src.routes.farmingareas"]

        module = importlib.import_module("src.routes.farmingareas")

        mock_generate.assert_called_once()
        kwargs = mock_generate.call_args.kwargs

        self.assertEqual(kwargs["prefix"], "/farmingareas")
        self.assertEqual(kwargs["tags"], ["Spatial data"])
        self.assertEqual(kwargs["collection"], module.FarmingAreas)
        self.assertEqual(kwargs["schema_model"], module.FarmingAreasSchema)
        self.assertEqual(kwargs["allowed_fields"], ["name"])
        self.assertEqual(kwargs["serialize_fn"], module.serialize_farming_areas)
        self.assertEqual(kwargs["include_endpoints"], ["paged", "by-name"])