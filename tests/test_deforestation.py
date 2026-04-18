import importlib
import sys
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.routes.deforestation import _to_iso, serialize_deforestation


class TestDeforestation(unittest.TestCase):

    def test_to_iso_returns_none_when_datetime_is_none(self):
        result = _to_iso(None)
        self.assertIsNone(result)

    def test_to_iso_returns_zulu_string_for_timezone_aware_datetime(self):
        dt = datetime(2025, 7, 7, 22, 0, 46, tzinfo=timezone.utc)

        result = _to_iso(dt)

        self.assertEqual(result, "2025-07-07T22:00:46Z")

    def test_serialize_deforestation_returns_expected_structure(self):
        doc = SimpleNamespace(
            id="def-id",
            deforestation_source=SimpleNamespace(value="SMBYC"),
            deforestation_type=SimpleNamespace(value="annual"),
            name="smbyc_deforestation_annual_2010_2012",
            period_start=datetime(2010, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            period_end=datetime(2012, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            path="deforestation/smbyc_deforestation_annual/",
            log=SimpleNamespace(
                enable=True,
                created=datetime(2025, 7, 7, 22, 0, 46, tzinfo=timezone.utc),
                updated=datetime(2025, 7, 7, 22, 0, 46, tzinfo=timezone.utc),
            ),
        )

        result = serialize_deforestation(doc)

        self.assertEqual(
            result,
            {
                "id": "def-id",
                "deforestation_source": "SMBYC",
                "deforestation_type": "annual",
                "name": "smbyc_deforestation_annual_2010_2012",
                "period_start": "2010-01-01T00:00:00Z",
                "period_end": "2012-12-31T23:59:59Z",
                "path": "deforestation/smbyc_deforestation_annual/",
                "log": {
                    "enable": True,
                    "created": "2025-07-07T22:00:46Z",
                    "updated": "2025-07-07T22:00:46Z",
                },
            },
        )

    def test_serialize_deforestation_uses_year_start_and_year_end_when_periods_are_missing(self):
        doc = SimpleNamespace(
            id="def-id",
            deforestation_source=SimpleNamespace(value="SMBYC"),
            deforestation_type=SimpleNamespace(value="cumulative"),
            name="legacy",
            period_start=None,
            period_end=None,
            year_start=2015,
            year_end=2017,
            path="legacy/path",
            log=None,
        )

        result = serialize_deforestation(doc)

        self.assertEqual(result["period_start"], "2015-01-01T00:00:00")
        self.assertEqual(result["period_end"], "2017-12-31T23:59:59")
        self.assertIsNone(result["log"])

    def test_serialize_deforestation_returns_none_for_optional_fields_when_missing(self):
        doc = SimpleNamespace(
            id="def-id",
            deforestation_source=None,
            deforestation_type=None,
            name=None,
            period_start=None,
            period_end=None,
            path=None,
            log=None,
        )

        result = serialize_deforestation(doc)

        self.assertEqual(
            result,
            {
                "id": "def-id",
                "deforestation_source": None,
                "deforestation_type": None,
                "name": None,
                "period_start": None,
                "period_end": None,
                "path": None,
                "log": None,
            },
        )

    @patch("src.routes.base_route.generate_read_only_router")
    def test_module_configures_read_only_router_with_expected_arguments(self, mock_generate):
        fake_router = MagicMock(name="inner_router")
        mock_generate.return_value = fake_router

        if "src.routes.deforestation" in sys.modules:
            del sys.modules["src.routes.deforestation"]

        module = importlib.import_module("src.routes.deforestation")

        mock_generate.assert_called_once()
        kwargs = mock_generate.call_args.kwargs

        self.assertEqual(kwargs["prefix"], "/deforestation")
        self.assertEqual(kwargs["tags"], ["Spatial data"])
        self.assertEqual(kwargs["collection"], module.Deforestation)
        self.assertEqual(kwargs["schema_model"], module.DeforestationSchema)
        self.assertEqual(kwargs["allowed_fields"], ["deforestation_source", "deforestation_type", "name"])
        self.assertEqual(kwargs["serialize_fn"], module.serialize_deforestation)
        self.assertEqual(kwargs["include_endpoints"], ["paged", "by-name"])