import importlib
import sys
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from ganabosques_orm.enums.valuechain import ValueChain
from src.routes.analysis import (
    _enum_or_str,
    _safe_period_end,
    _to_dt_or_none,
    serialize_analysis,
    get_all,
)


class TestAnalysis(unittest.TestCase):

    def test_enum_or_str_returns_none_when_value_is_none(self):
        result = _enum_or_str(None)
        self.assertIsNone(result)

    def test_enum_or_str_returns_enum_value_when_enum_is_provided(self):
        value = list(ValueChain)[0]

        result = _enum_or_str(value)

        self.assertEqual(result, value.value)

    def test_enum_or_str_returns_string_representation_for_plain_value(self):
        result = _enum_or_str("annual")
        self.assertEqual(result, "annual")

    def test_to_dt_or_none_returns_none_for_none(self):
        result = _to_dt_or_none(None)
        self.assertIsNone(result)

    def test_to_dt_or_none_returns_same_datetime_when_input_is_datetime(self):
        dt = datetime(2025, 1, 1, 10, 30, 0)

        result = _to_dt_or_none(dt)

        self.assertEqual(result, dt)

    def test_to_dt_or_none_parses_iso_string(self):
        result = _to_dt_or_none("2025-01-01T10:30:00Z")

        self.assertIsNotNone(result)
        self.assertEqual(result.year, 2025)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 1)

    def test_to_dt_or_none_returns_none_for_invalid_string(self):
        result = _to_dt_or_none("not-a-date")
        self.assertIsNone(result)

    def test_safe_period_end_returns_denormalized_date_when_present(self):
        dt = datetime(2024, 12, 31, 23, 59, 59)

        analysis = SimpleNamespace(
            deforestation_period_end=dt,
            deforestation_id=None,
        )

        result = _safe_period_end(analysis)

        self.assertEqual(result, dt)

    def test_safe_period_end_returns_deforestation_period_end_when_denormalized_missing(self):
        dt = datetime(2024, 6, 30, 0, 0, 0)

        analysis = SimpleNamespace(
            deforestation_period_end=None,
            deforestation_id=SimpleNamespace(period_end=dt),
        )

        result = _safe_period_end(analysis)

        self.assertEqual(result, dt)

    def test_safe_period_end_returns_datetime_min_when_no_period_end_exists(self):
        analysis = SimpleNamespace(
            deforestation_period_end=None,
            deforestation_id=SimpleNamespace(period_end=None),
        )

        result = _safe_period_end(analysis)

        self.assertEqual(result, datetime.min)

    def test_serialize_analysis_returns_expected_structure_with_deforestation_data(self):
        protected_area = SimpleNamespace(id="protected-id")
        farming_area = SimpleNamespace(id="farming-id")
        deforestation = SimpleNamespace(
            id="deforestation-id",
            deforestation_source="smbyc",
            deforestation_type="annual",
            name="smbyc_deforestation_annual_2010_2012",
            period_start=datetime(2010, 1, 1, 0, 0, 0),
            period_end=datetime(2012, 12, 31, 23, 59, 59),
            path="deforestation/smbyc_deforestation_annual/",
        )

        analysis = SimpleNamespace(
            id="analysis-id",
            protected_areas_id=protected_area,
            farming_areas_id=farming_area,
            deforestation_id=deforestation,
            user_id="user-id",
            date=datetime(2025, 6, 11, 14, 30, 0),
        )

        result = serialize_analysis(analysis)

        self.assertEqual(result["id"], "analysis-id")
        self.assertEqual(result["protected_areas_id"], "protected-id")
        self.assertEqual(result["farming_areas_id"], "farming-id")
        self.assertEqual(result["deforestation_id"], "deforestation-id")
        self.assertEqual(result["deforestation_source"], "smbyc")
        self.assertEqual(result["deforestation_type"], "annual")
        self.assertEqual(result["deforestation_name"], "smbyc_deforestation_annual_2010_2012")
        self.assertEqual(result["deforestation_path"], "deforestation/smbyc_deforestation_annual/")
        self.assertEqual(result["user_id"], "user-id")
        self.assertEqual(result["date"], "2025-06-11T14:30:00")

    def test_serialize_analysis_returns_none_values_when_optional_fields_are_missing(self):
        analysis = SimpleNamespace(
            id="analysis-id",
            protected_areas_id=None,
            farming_areas_id=None,
            deforestation_id=None,
            user_id=None,
            date=None,
        )

        result = serialize_analysis(analysis)

        self.assertEqual(
            result,
            {
                "id": "analysis-id",
                "protected_areas_id": None,
                "farming_areas_id": None,
                "deforestation_id": None,
                "deforestation_source": None,
                "deforestation_type": None,
                "deforestation_name": None,
                "deforestation_period_start": None,
                "deforestation_period_end": None,
                "deforestation_path": None,
                "user_id": None,
                "date": None,
            },
        )

    @patch("src.routes.analysis.Analysis")
    def test_get_all_returns_serialized_items_sorted_by_period_end_desc(self, mock_analysis):
        analysis_older = SimpleNamespace(
            id="a1",
            protected_areas_id=None,
            farming_areas_id=None,
            deforestation_id=SimpleNamespace(
                id="d1",
                deforestation_source="smbyc",
                deforestation_type="annual",
                name="older",
                period_start=datetime(2020, 1, 1, 0, 0, 0),
                period_end=datetime(2020, 12, 31, 23, 59, 59),
                path="path/older",
            ),
            user_id=None,
            date=None,
            deforestation_period_end=None,
        )

        analysis_newer = SimpleNamespace(
            id="a2",
            protected_areas_id=None,
            farming_areas_id=None,
            deforestation_id=SimpleNamespace(
                id="d2",
                deforestation_source="smbyc",
                deforestation_type="annual",
                name="newer",
                period_start=datetime(2022, 1, 1, 0, 0, 0),
                period_end=datetime(2022, 12, 31, 23, 59, 59),
                path="path/newer",
            ),
            user_id=None,
            date=None,
            deforestation_period_end=None,
        )

        query = MagicMock()
        query.select_related.return_value = [analysis_older, analysis_newer]
        mock_analysis.objects = query

        result = get_all(value_chain=None)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], "a2")
        self.assertEqual(result[1]["id"], "a1")
        query.select_related.assert_called_once()

    @patch("src.routes.analysis.Analysis")
    def test_get_all_applies_value_chain_filter_when_provided(self, mock_analysis):
        value_chain = list(ValueChain)[0]

        analysis_doc = SimpleNamespace(
            id="a1",
            protected_areas_id=None,
            farming_areas_id=None,
            deforestation_id=None,
            user_id=None,
            date=None,
            deforestation_period_end=None,
        )

        filtered_query = MagicMock()
        filtered_query.select_related.return_value = [analysis_doc]

        base_query = MagicMock()
        base_query.filter.return_value = filtered_query
        mock_analysis.objects = base_query

        result = get_all(value_chain=value_chain)

        self.assertEqual(len(result), 1)
        base_query.filter.assert_called_once_with(value_chain=value_chain.value)
        filtered_query.select_related.assert_called_once()

    @patch("src.routes.base_route.generate_read_only_router")
    def test_module_configures_read_only_router_with_expected_arguments(self, mock_generate_read_only_router):
        fake_router = MagicMock(name="inner_router")
        mock_generate_read_only_router.return_value = fake_router

        if "src.routes.analysis" in sys.modules:
            del sys.modules["src.routes.analysis"]

        module = importlib.import_module("src.routes.analysis")

        mock_generate_read_only_router.assert_called_once()
        kwargs = mock_generate_read_only_router.call_args.kwargs

        self.assertEqual(kwargs["prefix"], "/analysis")
        self.assertEqual(kwargs["tags"], ["Analysis risk"])
        self.assertEqual(kwargs["collection"], module.Analysis)
        self.assertEqual(kwargs["schema_model"], module.AnalysisSchema)
        self.assertEqual(kwargs["allowed_fields"], [])
        self.assertEqual(kwargs["serialize_fn"], module.serialize_analysis)
        self.assertEqual(kwargs["include_endpoints"], ["paged"])
        self.assertFalse(kwargs["include_get_all"])


if __name__ == "__main__":
    unittest.main()