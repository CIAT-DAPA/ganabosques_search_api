import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bson import ObjectId
from fastapi import HTTPException

from src.routes.get_analysis import get_analysis_by_deforestation


class TestGetAnalysis(unittest.TestCase):

    def test_get_analysis_by_deforestation_raises_http_exception_for_invalid_id(self):
        with self.assertRaises(HTTPException) as context:
            get_analysis_by_deforestation("bad-id")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "ID de deforestación inválido")

    @patch("src.routes.get_analysis.Analysis")
    def test_get_analysis_by_deforestation_raises_404_when_no_results(self, mock_analysis):
        deforestation_id = str(ObjectId())
        mock_analysis.objects.return_value.select_related.return_value = []

        with self.assertRaises(HTTPException) as context:
            get_analysis_by_deforestation(deforestation_id)

        self.assertEqual(context.exception.status_code, 404)
        self.assertEqual(
            context.exception.detail,
            "No se encontraron análisis con ese ID de deforestación"
        )

    @patch("src.routes.get_analysis.Analysis")
    def test_get_analysis_by_deforestation_returns_serialized_results(self, mock_analysis):
        deforestation_oid = ObjectId()
        analysis_oid = ObjectId()
        protected_oid = ObjectId()
        farming_oid = ObjectId()
        user_oid = ObjectId()

        doc = SimpleNamespace(
            id=analysis_oid,
            protected_areas_id=SimpleNamespace(id=protected_oid),
            farming_areas_id=SimpleNamespace(id=farming_oid),
            deforestation_id=SimpleNamespace(
                id=deforestation_oid,
                deforestation_source=SimpleNamespace(value="smbyc"),
                deforestation_type=SimpleNamespace(value="annual"),
                name="deforestation_2024",
                period_start=SimpleNamespace(isoformat=lambda: "2024-01-01T00:00:00"),
                period_end=SimpleNamespace(isoformat=lambda: "2024-12-31T23:59:59"),
                path="deforestation/path",
            ),
            user_id=user_oid,
            date=SimpleNamespace(isoformat=lambda: "2025-01-01T12:00:00"),
        )

        mock_analysis.objects.return_value.select_related.return_value = [doc]

        result = get_analysis_by_deforestation(str(deforestation_oid))

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result[0],
            {
                "id": str(analysis_oid),
                "protected_areas_id": str(protected_oid),
                "farming_areas_id": str(farming_oid),
                "deforestation_id": str(deforestation_oid),
                "deforestation_source": "smbyc",
                "deforestation_type": "annual",
                "deforestation_name": "deforestation_2024",
                "deforestation_period_start": "2024-01-01T00:00:00",
                "deforestation_period_end": "2024-12-31T23:59:59",
                "deforestation_path": "deforestation/path",
                "user_id": str(user_oid),
                "date": "2025-01-01T12:00:00",
            },
        )

    @patch("src.routes.get_analysis.Analysis")
    def test_get_analysis_by_deforestation_wraps_unexpected_errors(self, mock_analysis):
        deforestation_id = str(ObjectId())
        mock_analysis.objects.side_effect = Exception("db error")

        with self.assertRaises(HTTPException) as context:
            get_analysis_by_deforestation(deforestation_id)

        self.assertEqual(context.exception.status_code, 500)
        self.assertIn("Error interno: db error", context.exception.detail)