import re
from fastapi import Query, HTTPException
from typing import Optional, List, Dict, Literal, Union
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.movement import Movement
from ganabosques_orm.enums.species import Species
from ganabosques_orm.enums.typemovement import TypeMovement
from mongoengine.queryset.visitor import Q
import time
from tools.pagination import build_paginated_response, PaginatedResponse
from tools.logger import logger

from routes.base_route import generate_read_only_router
from routes.enterprise import EnterpriseSchema
from routes.farm import FarmSchema
from tools.utils import parse_object_ids, build_search_query

class ClassificationSchema(BaseModel):
    label: Optional[str] = Field(None, description="Label of classification")
    amount: Optional[int] = Field(None, description="Amount associated with label")

class MovementSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the movement")
    date: Optional[str] = Field(None, description="Date of the movement (ISO format)")
    type_origin: Optional[TypeMovement] = Field(None, description="Origin type")
    type_destination: Optional[TypeMovement] = Field(None, description="Destination type")
    source_movement: Optional[str] = Field(None, description="Origin source")
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
                "source_movement": "665f9999b1ac3457e3a91000",
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


# Estadística por categoría
class CategoryStatistics(BaseModel):
    headcount: int
    movements: int

# Species -> label -> CategoryStatistics
SpeciesStatistics = Dict[str, CategoryStatistics]

# Year -> species -> label -> stats
YearStatistics = Dict[str, SpeciesStatistics]

# farms/enterprises destino
class MovementDestination(BaseModel):
    direction: Literal["in", "out"]
    destination_type: str
    movements: int
    destination: Union[List[FarmSchema], List[EnterpriseSchema]]

# Grupo de movimientos
class MovementGroup(BaseModel):
    farms: List[MovementDestination]
    enterprises: List[MovementDestination]
    statistics: Optional[YearStatistics] = None

# Respuesta principal
class MovementStatisticsResponse(BaseModel):
    inputs: MovementGroup
    outputs: MovementGroup

def serialize_movement(doc):
    """Serialize a Movement document into a JSON-compatible dictionary."""
    return {
        "id": str(doc.id),
        "date": doc.date.isoformat() if doc.date else None,
        "type_origin": str(doc.type_origin.value) if doc.type_origin else None,
        "type_destination": str(doc.type_destination.value) if doc.type_destination else None,
        "source_movement": str(doc.source_movement.id) if doc.source_movement else None,
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

@router.get("/by-farmid", response_model=List[MovementSchema])
def get_movement_by_farmid(
    ids: str = Query(..., description="One or more comma-separated farm_id to filter movements records"),
    roles: Optional[str] = Query(
        None, description="Which role(s) to filter by: 'origin', 'destination', or both (default: both)",
    )
):
    """Search movement records by farm_id in origin, destination or both."""

    t0 = time.perf_counter()

    terms = parse_object_ids(ids)

    roles = [r.lower().strip() for r in roles.split(",")] if roles else ["origin", "destination"]

    t1 = time.perf_counter()
    query = None
    if "origin" in roles:
        query = Q(farm_id_origin__in=terms)
    if "destination" in roles:
        q_dest = Q(farm_id_destination__in=terms)
        query = q_dest if query is None else query | q_dest

    if query is None:
        raise HTTPException(status_code=400, detail="Invalid roles parameter: must include 'origin', 'destination', or both.")
    t2 = time.perf_counter()
    
    matches = Movement.objects.filter(query).only(
        "id", "date", "type_origin", "type_destination", "source_movement",
        "ext_id", "farm_id_origin", "farm_id_destination",
        "enterprise_id_origin", "enterprise_id_destination", "movement", "species"
    ).select_related()
    t3 = time.perf_counter()
    serialized = [serialize_movement(movement) for movement in matches]
    t4 = time.perf_counter()

    logger.info(
        f"/by-farmid timing (records={len(serialized)}): "
        f"Parse={((t1 - t0) * 1000):.2f}ms | "
        f"BuildQuery={((t2 - t1) * 1000):.2f}ms | "
        f"QueryExec={((t3 - t2) * 1000):.2f}ms | "
        f"Serialize={((t4 - t3) * 1000):.2f}ms | "
        f"Total={((t4 - t0) * 1000):.2f}ms"
    )
    return serialized


@router.get("/by-enterpriseid", response_model=List[MovementSchema])
def get_movement_by_enterpriseid(
    ids: str = Query(..., description="One or more comma-separated enterprise_id to filter movements records"),
    roles: Optional[str] = Query(
        None, description="Which role(s) to filter by: 'origin', 'destination', or both (default: both)",
    )
):
    """Search movement records by enterprise_id in origin, destination or both."""
    t0 = time.perf_counter()

    terms = parse_object_ids(ids)

    roles = [r.lower().strip() for r in roles.split(",")] if roles else ["origin", "destination"]

    t1 = time.perf_counter()
    query = None
    if "origin" in roles:
        query = Q(enterprise_id_origin__in=terms)
    if "destination" in roles:
        q_dest = Q(enterprise_id_destination__in=terms)
        query = q_dest if query is None else query | q_dest

    if query is None:
        raise HTTPException(status_code=400, detail="Invalid roles parameter: must include 'origin', 'destination', or both.")
    t2 = time.perf_counter()
    matches = Movement.objects(query).select_related()
    t3 = time.perf_counter()
    serialized = [serialize_movement(movement) for movement in matches]
    t4 = time.perf_counter()

    logger.info(
        f"/by-farmid timing (records={len(serialized)}): "
        f"Parse={((t1 - t0) * 1000):.2f}ms | "
        f"BuildQuery={((t2 - t1) * 1000):.2f}ms | "
        f"QueryExec={((t3 - t2) * 1000):.2f}ms | "
        f"Serialize={((t4 - t3) * 1000):.2f}ms | "
        f"Total={((t4 - t0) * 1000):.2f}ms"
    )
    return serialized

@router.get("/statistics-by-farmid", response_model=MovementStatisticsResponse)
def get_movement_by_farmidtest(
    ids: str = Query(..., description="One farm_id to filter movements records"),
):
    """Search statistics of movements by farm_id i"""

    t0 = time.perf_counter()

    terms = parse_object_ids(ids)

    t1 = time.perf_counter()

    farm_id = ObjectId(terms[0])

    pipeline = [
        {
            "$match": {
                "$or": [
                    { "farm_id_origin": farm_id },
                    { "farm_id_destination": farm_id }
                ]
            }
        },
        {
            "$facet": {
                "farms_out": [
                    { "$match": { "farm_id_origin": farm_id, "type_destination": "FARM" } },
                    { "$group": { "_id": "$farm_id_destination", "movements": { "$sum": 1 }, "type_destination": { "$first": "$type_destination" } } },
                    { "$lookup": { "from": "farm", "localField": "_id", "foreignField": "_id", "as": "destination_info" } },
                    { "$unwind": "$destination_info" },
                    { "$project": { "_id": 0, "direction": "out", "destination_type": "$type_destination", "movements": 1, "destination": "$destination_info" } }
                ],
                "farms_in": [
                    { "$match": { "farm_id_destination": farm_id, "type_origin": "FARM" } },
                    { "$group": { "_id": "$farm_id_origin", "movements": { "$sum": 1 }, "type_destination": { "$first": "$type_origin" } } },
                    { "$lookup": { "from": "farm", "localField": "_id", "foreignField": "_id", "as": "destination_info" } },
                    { "$unwind": "$destination_info" },
                    { "$project": { "_id": 0, "direction": "in", "destination_type": "$type_destination", "movements": 1, "destination": "$destination_info" } }
                ],
                "enterprises_out": [
                    { "$match": { "farm_id_origin": farm_id, "type_destination": { "$ne": "FARM" } } },
                    { "$group": { "_id": "$enterprise_id_destination", "movements": { "$sum": 1 }, "type_destination": { "$first": "$type_destination" } } },
                    { "$lookup": { "from": "enterprise", "localField": "_id", "foreignField": "_id", "as": "destination_info" } },
                    { "$unwind": "$destination_info" },
                    { "$project": { "_id": 0, "direction": "out", "destination_type": "$type_destination", "movements": 1, "destination": "$destination_info" } }
                ],
                "enterprises_in": [
                    { "$match": { "farm_id_destination": farm_id, "type_origin": { "$ne": "FARM" } } },
                    { "$group": { "_id": "$enterprise_id_origin", "movements": { "$sum": 1 }, "type_destination": { "$first": "$type_origin" } } },
                    { "$lookup": { "from": "enterprise", "localField": "_id", "foreignField": "_id", "as": "destination_info" } },
                    { "$unwind": "$destination_info" },
                    { "$project": { "_id": 0, "direction": "in", "destination_type": "$type_destination", "movements": 1, "destination": "$destination_info" } }
                ],
                "statistic_out": [
                    { "$match": { "farm_id_origin": farm_id } },
                    { "$unwind": "$movement" },
                    { "$group": {
                        "_id": {
                            "year": { "$year": "$date" },
                            "species": "$species",
                            "label": "$movement.label"
                        },
                        "headcount": { "$sum": "$movement.amount" },
                        "movements": { "$sum": 1 }
                    }},
                    { "$group": {
                        "_id": { "year": "$_id.year", "species": "$_id.species" },
                        "labels": { "$push": { "k": "$_id.label", "v": { "headcount": "$headcount", "movements": "$movements" } } }
                    }},
                    { "$group": {
                        "_id": "$_id.year",
                        "species": { "$push": { "k": "$_id.species", "v": { "$arrayToObject": "$labels" } } }
                    }},
                    { "$project": { "k": { "$toString": "$_id" }, "v": { "$arrayToObject": "$species" } } },
                    { "$replaceRoot": { "newRoot": { "k": "$k", "v": "$v" } } },
                    { "$group": { "_id": None, "statistics": { "$push": { "k": "$k", "v": "$v" } } } },
                    { "$project": { "_id": 0, "statistics": { "$arrayToObject": "$statistics" } } }
                ],
                "statistic_in": [
                    { "$match": { "farm_id_destination": farm_id } },
                    { "$unwind": "$movement" },
                    { "$group": {
                        "_id": {
                            "year": { "$year": "$date" },
                            "species": "$species",
                            "label": "$movement.label"
                        },
                        "headcount": { "$sum": "$movement.amount" },
                        "movements": { "$sum": 1 }
                    }},
                    { "$group": {
                        "_id": { "year": "$_id.year", "species": "$_id.species" },
                        "labels": { "$push": { "k": "$_id.label", "v": { "headcount": "$headcount", "movements": "$movements" } } }
                    }},
                    { "$group": {
                        "_id": "$_id.year",
                        "species": { "$push": { "k": "$_id.species", "v": { "$arrayToObject": "$labels" } } }
                    }},
                    { "$project": { "k": { "$toString": "$_id" }, "v": { "$arrayToObject": "$species" } } },
                    { "$replaceRoot": { "newRoot": { "k": "$k", "v": "$v" } } },
                    { "$group": { "_id": None, "statistics": { "$push": { "k": "$k", "v": "$v" } } } },
                    { "$project": { "_id": 0, "statistics": { "$arrayToObject": "$statistics" } } }
                ]
            }
        },
        {
            "$project": {
                "_id": 0,
                "inputs": {
                    "farms": "$farms_in",
                    "enterprises": "$enterprises_in",
                    "statistics": { "$arrayElemAt": ["$statistic_in.statistics", 0] }
                },
                "outputs": {
                    "farms": "$farms_out",
                    "enterprises": "$enterprises_out",
                    "statistics": { "$arrayElemAt": ["$statistic_out.statistics", 0] }
                }
            }
        }
    ]

    t2 = time.perf_counter()

    matches = list(Movement.objects.aggregate(pipeline))

    t3 = time.perf_counter()

    # Devolver la estructura deseada
    result = convert_object_ids(matches[0] if matches else {"inputs": {}, "outputs": {}})

    t4 = time.perf_counter()

    logger.info(
        #f"/by-farmid timing (records={len(result.farms) + len(result.enterprises)}): "
        f"Parse={((t1 - t0) * 1000):.2f}ms | "
        f"BuildQuery={((t2 - t1) * 1000):.2f}ms | "
        f"QueryExec={((t3 - t2) * 1000):.2f}ms | "
        f"Serialize={((t4 - t3) * 1000):.2f}ms | "
        f"Total={((t4 - t0) * 1000):.2f}ms"
    )

    # Devolver la estructura deseada
    return result

@router.get("/statistics-by-enterpriseid", response_model=MovementStatisticsResponse)
def get_movement_by_farmidtest(
    ids: str = Query(..., description="One enterprise_id to filter movements records"),
):
    """Search statistics of movements by enterprise_id i"""

    t0 = time.perf_counter()

    terms = parse_object_ids(ids)

    t1 = time.perf_counter()

    enterprise_id = ObjectId(terms[0])

    pipeline = [
        {
            "$match": {
                "$or": [
                    { "enterprise_id_origin": enterprise_id },
                    { "enterprise_id_destination": enterprise_id }
                ]
            }
        },
        {
            "$facet": {
                "farms_out": [
                    { "$match": { "enterprise_id_origin": enterprise_id, "type_destination": "FARM" } },
                    { "$group": { "_id": "$farm_id_destination", "movements": { "$sum": 1 }, "type_destination": { "$first": "$type_destination" } } },
                    { "$lookup": { "from": "farm", "localField": "_id", "foreignField": "_id", "as": "destination_info" } },
                    { "$unwind": "$destination_info" },
                    { "$project": { "_id": 0, "direction": "out", "destination_type": "$type_destination", "movements": 1, "destination": "$destination_info" } }
                ],
                "farms_in": [
                    { "$match": { "enterprise_id_destination": enterprise_id, "type_origin": "FARM" } },
                    { "$group": { "_id": "$farm_id_origin", "movements": { "$sum": 1 }, "type_destination": { "$first": "$type_origin" } } },
                    { "$lookup": { "from": "farm", "localField": "_id", "foreignField": "_id", "as": "destination_info" } },
                    { "$unwind": "$destination_info" },
                    { "$project": { "_id": 0, "direction": "in", "destination_type": "$type_destination", "movements": 1, "destination": "$destination_info" } }
                ],
                "enterprises_out": [
                    { "$match": { "enterprise_id_origin": enterprise_id, "type_destination": { "$ne": "FARM" } } },
                    { "$group": { "_id": "$enterprise_id_destination", "movements": { "$sum": 1 }, "type_destination": { "$first": "$type_destination" } } },
                    { "$lookup": { "from": "enterprise", "localField": "_id", "foreignField": "_id", "as": "destination_info" } },
                    { "$unwind": "$destination_info" },
                    { "$project": { "_id": 0, "direction": "out", "destination_type": "$type_destination", "movements": 1, "destination": "$destination_info" } }
                ],
                "enterprises_in": [
                    { "$match": { "enterprise_id_destination": enterprise_id, "type_origin": { "$ne": "FARM" } } },
                    { "$group": { "_id": "$enterprise_id_origin", "movements": { "$sum": 1 }, "type_destination": { "$first": "$type_origin" } } },
                    { "$lookup": { "from": "enterprise", "localField": "_id", "foreignField": "_id", "as": "destination_info" } },
                    { "$unwind": "$destination_info" },
                    { "$project": { "_id": 0, "direction": "in", "destination_type": "$type_destination", "movements": 1, "destination": "$destination_info" } }
                ],
                "statistic_out": [
                    { "$match": { "enterprise_id_origin": enterprise_id } },
                    { "$unwind": "$movement" },
                    { "$group": {
                        "_id": {
                            "year": { "$year": "$date" },
                            "species": "$species",
                            "label": "$movement.label"
                        },
                        "headcount": { "$sum": "$movement.amount" },
                        "movements": { "$sum": 1 }
                    }},
                    { "$group": {
                        "_id": { "year": "$_id.year", "species": "$_id.species" },
                        "labels": { "$push": { "k": "$_id.label", "v": { "headcount": "$headcount", "movements": "$movements" } } }
                    }},
                    { "$group": {
                        "_id": "$_id.year",
                        "species": { "$push": { "k": "$_id.species", "v": { "$arrayToObject": "$labels" } } }
                    }},
                    { "$project": { "k": { "$toString": "$_id" }, "v": { "$arrayToObject": "$species" } } },
                    { "$replaceRoot": { "newRoot": { "k": "$k", "v": "$v" } } },
                    { "$group": { "_id": None, "statistics": { "$push": { "k": "$k", "v": "$v" } } } },
                    { "$project": { "_id": 0, "statistics": { "$arrayToObject": "$statistics" } } }
                ],
                "statistic_in": [
                    { "$match": { "enterprise_id_destination": enterprise_id } },
                    { "$unwind": "$movement" },
                    { "$group": {
                        "_id": {
                            "year": { "$year": "$date" },
                            "species": "$species",
                            "label": "$movement.label"
                        },
                        "headcount": { "$sum": "$movement.amount" },
                        "movements": { "$sum": 1 }
                    }},
                    { "$group": {
                        "_id": { "year": "$_id.year", "species": "$_id.species" },
                        "labels": { "$push": { "k": "$_id.label", "v": { "headcount": "$headcount", "movements": "$movements" } } }
                    }},
                    { "$group": {
                        "_id": "$_id.year",
                        "species": { "$push": { "k": "$_id.species", "v": { "$arrayToObject": "$labels" } } }
                    }},
                    { "$project": { "k": { "$toString": "$_id" }, "v": { "$arrayToObject": "$species" } } },
                    { "$replaceRoot": { "newRoot": { "k": "$k", "v": "$v" } } },
                    { "$group": { "_id": None, "statistics": { "$push": { "k": "$k", "v": "$v" } } } },
                    { "$project": { "_id": 0, "statistics": { "$arrayToObject": "$statistics" } } }
                ]
            }
        },
        {
            "$project": {
                "_id": 0,
                "inputs": {
                    "farms": "$farms_in",
                    "enterprises": "$enterprises_in",
                    "statistics": { "$arrayElemAt": ["$statistic_in.statistics", 0] }
                },
                "outputs": {
                    "farms": "$farms_out",
                    "enterprises": "$enterprises_out",
                    "statistics": { "$arrayElemAt": ["$statistic_out.statistics", 0] }
                }
            }
        }
    ]

    t2 = time.perf_counter()

    matches = list(Movement.objects.aggregate(pipeline))

    t3 = time.perf_counter()

    # Devolver la estructura deseada
    result = convert_object_ids(matches[0] if matches else {"inputs": {}, "outputs": {}})

    t4 = time.perf_counter()

    logger.info(
        #f"/by-farmid timing (records={len(result.farms) + len(result.enterprises)}): "
        f"Parse={((t1 - t0) * 1000):.2f}ms | "
        f"BuildQuery={((t2 - t1) * 1000):.2f}ms | "
        f"QueryExec={((t3 - t2) * 1000):.2f}ms | "
        f"Serialize={((t4 - t3) * 1000):.2f}ms | "
        f"Total={((t4 - t0) * 1000):.2f}ms"
    )

    # Devolver la estructura deseada
    return result


def convert_object_ids(obj):
    if isinstance(obj, dict):
        return {k: convert_object_ids(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_object_ids(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj