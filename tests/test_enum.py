import unittest
from enum import Enum
from unittest.mock import patch

from fastapi import HTTPException

from src.routes.enum import read_enum


class TestEnum(unittest.TestCase):

    @patch("src.routes.enum.import_module")
    def test_read_enum_returns_member_values_when_enum_exists(self, mock_import_module):
        class Species(Enum):
            COW = "cow"
            PIG = "pig"

        fake_module = type("FakeModule", (), {"Species": Species})
        mock_import_module.return_value = fake_module

        result = read_enum("species")

        self.assertEqual(result, ["cow", "pig"])
        mock_import_module.assert_called_once_with("ganabosques_orm.enums.species")

    @patch("src.routes.enum.import_module")
    def test_read_enum_raises_http_exception_when_module_is_missing(self, mock_import_module):
        mock_import_module.side_effect = ModuleNotFoundError("not found")

        with self.assertRaises(HTTPException) as context:
            read_enum("species")

        self.assertEqual(context.exception.status_code, 404)
        self.assertIn("Failed to import module", context.exception.detail)

    @patch("src.routes.enum.import_module")
    def test_read_enum_raises_http_exception_when_class_is_missing(self, mock_import_module):
        fake_module = type("FakeModule", (), {})
        mock_import_module.return_value = fake_module

        with self.assertRaises(HTTPException) as context:
            read_enum("species")

        self.assertEqual(context.exception.status_code, 404)
        self.assertIn("Enum class 'Species' not found", context.exception.detail)

    @patch("src.routes.enum.import_module")
    def test_read_enum_raises_http_exception_when_object_is_not_enum(self, mock_import_module):
        fake_module = type("FakeModule", (), {"Species": str})
        mock_import_module.return_value = fake_module

        with self.assertRaises(HTTPException) as context:
            read_enum("species")

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("is not an Enum", context.exception.detail)