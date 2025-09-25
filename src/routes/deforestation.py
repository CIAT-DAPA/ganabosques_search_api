from datetime import datetime
from typing import Optional, List
from fastapi import Query, HTTPException
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.deforestation import Deforestation
from tools.pagination import build_paginated_response, PaginatedResponse
from schemas.logschema import LogSchema

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, build_search_query

class DeforestationSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the deforestation record")
    deforestation_source: str = Field(..., description="Source of the deforestation data (e.g., SMBYC)")
    deforestation_type: str = Field(..., description="Type of deforestation data: annual or cumulative")
    name: Optional[str] = Field(None, description="Name or label for the deforestation data")

    # ⬇️ ahora como DateTime; años legacy quedan manejados en el serializer
    period_start: Optional[datetime] = Field(
        None, description="Start datetime of the deforestation period"
    )
    period_end: Optional[datetime] = Field(
        None, description="End datetime of the deforestation period"
    )

    path: Optional[str] = Field(None, description="Path or reference to the deforestation file in Geoserver")
    log: Optional[LogSchema] = Field(None, description="Logging information")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "665fdef0b1ac3457e3a91b01",
                "deforestation_source": "SMBYC",
                "deforestation_type": "annual",
                "name": "smbyc_deforestation_annual_2010_2012",
                "period_start": "2010-01-01T00:00:00Z",
                "period_end": "2012-12-31T23:59:59Z",
                "path": "deforestation/smbyc_deforestation_annual/",
                "log": {
                    "enable": True,
                    "created": "2025-07-07T22:00:46.667Z",
                    "updated": "2025-07-07T22:00:46.667Z"
                }
            }
        }

def _to_iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat().replace("+00:00", "Z") if dt else None

def serialize_deforestation(doc):
    """Serialize a Deforestation document into a JSON-compatible dictionary."""
    # Compatibilidad por si quedan documentos antiguos con year_start/year_end
    period_start = getattr(doc, "period_start", None)
    period_end = getattr(doc, "period_end", None)

    # Si el ORM/Documento aún trae años sueltos como int, conviértelos a datetimes standard
    if (period_start is None) and hasattr(doc, "period_start"):
        # ya es None explícito
        pass
    if (period_end is None) and hasattr(doc, "period_end"):
        pass

    # Soporte legacy: si existieran year_start/year_end como int
    if period_start is None and hasattr(doc, "year_start") and doc.year_start is not None:
        period_start = datetime(int(doc.year_start), 1, 1)
    if period_end is None and hasattr(doc, "year_end") and doc.year_end is not None:
        period_end = datetime(int(doc.year_end), 12, 31, 23, 59, 59)

    return {
        "id": str(doc.id),
        "deforestation_source": str(doc.deforestation_source.value) if getattr(doc, "deforestation_source", None) else None,
        "deforestation_type": str(doc.deforestation_type.value) if getattr(doc, "deforestation_type", None) else None,
        "name": getattr(doc, "name", None),
        "period_start": _to_iso(period_start),
        "period_end": _to_iso(period_end),
        "path": getattr(doc, "path", None),
        "log": (
            {
                "enable": getattr(doc.log, "enable", None),
                "created": _to_iso(getattr(doc.log, "created", None)),
                "updated": _to_iso(getattr(doc.log, "updated", None)),
            }
            if getattr(doc, "log", None) else None
        ),
    }

router = generate_read_only_router(
    prefix="/deforestation",
    tags=["Spatial data"],
    collection=Deforestation,
    schema_model=DeforestationSchema,
    allowed_fields=["deforestation_source", "deforestation_type", "name"],  # añade 'period_*' si vas a filtrar por fechas
    serialize_fn=serialize_deforestation,
    include_endpoints=["paged", "by-name"]
)