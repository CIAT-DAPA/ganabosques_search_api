import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bson import DBRef, ObjectId
from fastapi import HTTPException

from src.routes.farmrisk_paginated import (
    _area,
    _as_object_id,
    _iso,
    get_farmrisk_by_analysis_id_page,
)


class TestFarmRiskPaginated(unittest.TestCase):

    def test_as_object_id_returns_none_for_none(self):
        self.assertIsNone(_as_object_id(None))

    def test_as_object_id_returns_same_objectid(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id(oid), oid)

    def test_as_object_id_returns_dbref_id(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id(DBRef("x", oid)), oid)

    def test_as_object_id_returns_dict_dollar_id_objectid(self):
        oid = ObjectId()
        self.assertEqual(_as_object_id({"$id": oid}), oid)

    def test_as_object_id_returns_none_for_invalid_value(self):
        self.assertIsNone(_as_object_id("bad-id"))

    def test_iso_returns_none_for_invalid_value(self):
        self.assertIsNone(_iso("not-date"))

    def test_area_returns_none_for_non_dict(self):
        self.assertIsNone(_area(None))

    def test_area_returns_risk_area_item_for_dict(self):
        result = _area({"ha": 10, "prop": 0.25})
        self.assertEqual(result.ha, 10.0)
        self.assertEqual(result.prop, 0.25)

    def test_get_farmrisk_by_analysis_id_page_raises_for_invalid_analysis_id(self):
        with self.assertRaises(HTTPException) as context:
            get_farmrisk_by_analysis_id_page(
                analysis_id="bad-id",
                page=1,
                page_size=20,
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Invalid ObjectId", context.exception.detail)

    @patch("src.routes.farmrisk_paginated.FarmRisk")
    def test_get_farmrisk_by_analysis_id_page_returns_empty_page_when_no_docs(self, mock_farmrisk):
        analysis_oid = ObjectId()

        coll = MagicMock()
        coll.find.return_value.sort.return_value.skip.return_value.limit.return_value = []
        mock_farmrisk._get_collection.return_value = coll

        result = get_farmrisk_by_analysis_id_page(
            analysis_id=str(analysis_oid),
            page=1,
            page_size=20,
        )

        self.assertEqual(result.page, 1)
        self.assertEqual(result.page_size, 20)
        self.assertEqual(result.items, [])

    @patch("src.routes.farmrisk_paginated.Adm3")
    @patch("src.routes.farmrisk_paginated.FarmPolygons")
    @patch("src.routes.farmrisk_paginated.Farm")
    @patch("src.routes.farmrisk_paginated.FarmRisk")
    def test_get_farmrisk_by_analysis_id_page_returns_enriched_items(
        self,
        mock_farmrisk,
        mock_farm,
        mock_farmpolygons,
        mock_adm3,
    ):
        analysis_oid = ObjectId()
        farm_oid = ObjectId()
        polygon_oid = ObjectId()
        adm3_oid = ObjectId()

        coll_risk = MagicMock()
        coll_risk.find.return_value.sort.return_value.skip.return_value.limit.return_value = [
            {
                "_id": ObjectId(),
                "farm_id": farm_oid,
                "farm_polygons_id": polygon_oid,
                "risk_direct": True,
                "risk_input": False,
                "risk_output": True,
                "deforestation": {"ha": 3.5, "prop": 0.4},
                "farming_in": {"ha": 1.0, "prop": 0.1},
                "farming_out": {"ha": 0.5, "prop": 0.05},
                "protected": {"ha": 2.0, "prop": 0.2},
            }
        ]
        mock_farmrisk._get_collection.return_value = coll_risk

        farm_doc = MagicMock()
        farm_doc.to_mongo.return_value.to_dict.return_value = {
            "_id": farm_oid,
            "adm3_id": adm3_oid,
            "ext_id": [{"source": "SIT_CODE", "ext_code": "S1"}],
            "log": {"enable": True},
        }
        farm_qs = MagicMock()
        farm_qs.no_dereference.return_value = farm_qs
        farm_qs.__iter__.return_value = iter([farm_doc])
        mock_farm.objects.return_value = farm_qs

        coll_poly = MagicMock()
        coll_poly.find.return_value = [
            {
                "farm_id": farm_oid,
                "latitude": 1.23,
                "longitud": -2.34,
                "geojson": {"type": "Polygon"},
            }
        ]
        mock_farmpolygons._get_collection.return_value = coll_poly

        adm3_doc = SimpleNamespace(id=adm3_oid, label="DEP, MUN, VEREDA")
        adm3_qs = MagicMock()
        adm3_qs.no_dereference.return_value = adm3_qs
        adm3_qs.only.return_value = [adm3_doc]
        mock_adm3.objects.return_value = adm3_qs

        result = get_farmrisk_by_analysis_id_page(
            analysis_id=str(analysis_oid),
            page=1,
            page_size=20,
        )

        self.assertEqual(result.page, 1)
        self.assertEqual(result.page_size, 20)
        self.assertEqual(len(result.items), 1)

        item = result.items[0]
        self.assertEqual(item.analysis_id, str(analysis_oid))
        self.assertEqual(item.farm_id, str(farm_oid))
        self.assertEqual(item.farm_polygons_id, str(polygon_oid))
        self.assertTrue(item.risk_direct)
        self.assertTrue(item.risk_output)
        self.assertEqual(item.deforestation.ha, 3.5)
        self.assertEqual(item.farm.adm3_id, str(adm3_oid))
        self.assertEqual(item.farm.adm3_name, "DEP, MUN, VEREDA")
        self.assertEqual(item.farm.latitude, 1.23)
        self.assertEqual(item.farm.longitud, -2.34)
        self.assertEqual(item.farm.geojson, {"type": "Polygon"})