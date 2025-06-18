import re
from fastapi import Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.movement import Movement
from ganabosques_orm.enums.species import Species
from ganabosques_orm.enums.typemovement import TypeMovement
from tools.pagination import build_paginated_response, PaginatedResponse

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, build_search_query

class SourceMovementSchema(BaseModel):
    id: Optional[str] = Field(None, description="Source MongoDB ObjectId")
    name: Optional[str] = Field(None, description="Name of the source")


class ClassificationSchema(BaseModel):
    label: Optional[str] = Field(None, description="Label of classification")
    amount: Optional[int] = Field(None, description="Amount associated with label")


class MovementSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the movement")
    date: Optional[str] = Field(None, description="Date of the movement (ISO format)")
    type_origin: Optional[TypeMovement] = Field(None, description="Origin type")
    type_destination: Optional[TypeMovement] = Field(None, description="Destination type")
    source: Optional[SourceMovementSchema] = Field(None, description="Origin source")
    ext_id: Optional[str] = Field(None, description="External ID")
    farm_id_origin: Optional[str] = Field(None, description="Origin farm ID")
    farm_id_destination: Optional[str] = Field(None, description="Destination farm ID")
    enterprise_id_origin: Optional[str] = Field(None, description="Origin enterprise ID")
    enterprise_id_destination: Optional[str] = Field(None, description="Destination enterprise ID")
    movement: List[ClassificationSchema] = Field(default_factory=list, description="Classification movements")
    species: Species = Field(..., description="Species involved in the movement")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "6661c001e2ac3457e3a93fff",
                "date": "2024-06-01T00:00:00Z",
                "type_origin": "FARM",
                "type_destination": "ENTERPRISE",
                "source": {
                    "id": "665f9999b1ac3457e3a91000",
                    "name": "Origen Ganadero"
                },
                "ext_id": "MV123456",
                "farm_id_origin": "665f1726b1ac3457e3a91a05",
                "farm_id_destination": "665f1726b1ac3457e3a91a07",
                "enterprise_id_origin": "665f1726b1ac3457e3a91a55",
                "enterprise_id_destination": "665f1726b1ac3457e3a91a99",
                "movement": [
                    {"label": "terneros", "amount": "15"},
                    {"label": "vacas", "amount": "10"}
                ],
                "species": "bovinos"
            }
        }

def serialize_movement(doc):
    """Serialize a Movement document into a JSON-compatible dictionary."""
    return {
        "id": str(doc.id),
        "date": doc.date.isoformat() if doc.date else None,
        "type_origin": str(doc.type_origin.value) if doc.type_origin else None,
        "type_destination": str(doc.type_destination.value) if doc.type_destination else None,
        "source": {
            "id": str(doc.source.id) if doc.source and doc.source.id else None,
            "name": doc.source.name if doc.source else None
        } if doc.source else None,
        "ext_id": doc.ext_id,
        "farm_id_origin": str(doc.farm_id_origin.id) if doc.farm_id_origin else None,
        "farm_id_destination": str(doc.farm_id_destination.id) if doc.farm_id_destination else None,
        "enterprise_id_origin": str(doc.enterprise_id_origin.id) if doc.enterprise_id_origin else None,
        "enterprise_id_destination": str(doc.enterprise_id_destination.id) if doc.enterprise_id_destination else None,
        "movement": [
            {"label": c.label, "amount": c.amount} for c in (doc.movement or [])
        ],
        "species": str(doc.species.value) if doc.species else None
    }

router = generate_read_only_router(
    prefix="/movement",
    tags=["Movement"],
    collection=Movement,
    schema_model=MovementSchema,
    allowed_fields=["ext_id", "species", "type_origin", "type_destination"],
    serialize_fn=serialize_movement,
    include_endpoints=["paged", "by-extid"]
)

# @router.get("/by-extid", response_model=List[MovementSchema])
# def get_movement_by_extid(
#     ext_ids: str = Query(..., description="One or more comma-separated ext_id for case-insensitive partial search")
# ):
#     """Search movement records by ext_id with partial, case-insensitive match."""
#     terms = [term.strip() for term in ext_ids.split(",") if term.strip()]
#     query = build_search_query(terms, ["ext_id"])
#     matches = Movement.objects(__raw__=query)
#     return [serialize_movement(movement) for movement in matches]