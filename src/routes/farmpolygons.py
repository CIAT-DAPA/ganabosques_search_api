import time
from fastapi import Query, HTTPException, Depends, APIRouter
from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict
from ganabosques_orm.collections.farmpolygons import FarmPolygons
from schemas.logschema import LogSchema

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, convert_doc_to_json

from ganabosques_orm.enums.ugg import UGG
from ganabosques_orm.enums.species import Species
from dependencies.auth_guard import require_admin


class BufferPolygonSchema(BaseModel):
    ugg: Optional[UGG] = Field(None, description="UGG category of the animals")
    amount: Optional[int] = Field(None, description="Quantity of animals in this UGG category")
    species: Optional[Species] = Field(None, description="Species of the animals in this buffer")


class FarmPolygonsSchema(BaseModel):
    id: str = Field(..., description="Internal MongoDB ID of the FarmPolygons")
    farm_id: Optional[str] = Field(None, description="MongoDB ID reference to the Farm document")
    geojson: Optional[str] = Field(None, description="GeoJSON geometry string of the polygon")
    latitude: Optional[float] = Field(None, description="Latitude of the polygon centroid")
    longitud: Optional[float] = Field(None, description="Longitude of the polygon centroid")
    farm_ha: Optional[float] = Field(None, description="Hectares of the farm polygon")
    radio: Optional[float] = Field(None, description="Buffer radius used")
    buffer_inputs: Optional[List[BufferPolygonSchema]] = Field(None, description="List of buffer polygon objects")
    log: Optional[LogSchema] = Field(None, description="Logging information")

    model_config = ConfigDict(
        from_attributes=True,
        use_enum_values=True,
        json_schema_extra={
            "example": {
                "id": "665f1726b1ac3457e3a91a01",
                "farm_id": "665f1234b1ac3457e3a90000",
                "geojson": "{\"type\":\"Polygon\",...}",
                "latitude": 3.4516,
                "longitud": -76.5320,
                "farm_ha": 12.5,
                "radio": 500.0,
                "buffer_inputs": [
                    {
                        "ugg": "TERNEROS_MENORES_1_ANIO",
                        "amount": 20,
                        "species": "BOVINOS"
                    }
                ],
                "log": {
                    "enable": True,
                    "created": "2024-01-01T10:00:00Z",
                    "updated": "2024-06-01T08:30:00Z"
                }
            }
        }
    )


_inner_router = generate_read_only_router(
    prefix="/farmpolygons",
    tags=["Farm and Enterprise"],
    collection=FarmPolygons,
    schema_model=FarmPolygonsSchema,
    allowed_fields=[],
    serialize_fn=None,  # Pydantic + convert_doc_to_json
    include_endpoints=["paged"],
    include_get_all=False
)


@_inner_router.get("/", response_model=List[FarmPolygonsSchema])
def get_all_farmpolygons_optimized():
    """
    Retrieve all FarmPolygons records.
    WARNING: This endpoint returns all polygons. Use /paged/ endpoint for better performance.
    """
    try:
        inicio_query = time.perf_counter()
        docs = list(FarmPolygons.objects().as_pymongo())
        fin_query = time.perf_counter()

        inicio_serialization = time.perf_counter()
        items = [convert_doc_to_json(doc) for doc in docs]
        fin_serialization = time.perf_counter()

        query_time = fin_query - inicio_query
        serialization_time = fin_serialization - inicio_serialization

        print(f"[FarmPolygons GET /] Query time: {query_time:.3f}s | Serialization time: {serialization_time:.3f}s | Total: {(query_time + serialization_time):.3f}s | Records: {len(items)}")

        return items
    except Exception as e:
        print(f"[FarmPolygons GET /] ERROR: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving all farmpolygons: {str(e)}"
        )


@_inner_router.get("/by-farm", response_model=List[FarmPolygonsSchema])
def get_farmpolygons_by_farm_ids(
    ids: str = Query(..., description="Comma-separated Farm IDs to filter FarmPolygonss records")
):
    try:
        search_ids = parse_object_ids(ids)

        inicio = time.perf_counter()
        docs = list(FarmPolygons.objects(farm_id__in=search_ids).as_pymongo())
        items = [convert_doc_to_json(doc) for doc in docs]
        fin = time.perf_counter()

        print(f"[FarmPolygons /by-farm] Time: {(fin - inicio):.3f}s | Records: {len(items)}")

        return items
    except HTTPException:
        raise
    except Exception as e:
        print(f"[FarmPolygons /by-farm] ERROR: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving farmpolygons by farm ids: {str(e)}"
        )


router = APIRouter(
    dependencies=[Depends(require_admin)]
)

router.include_router(_inner_router)