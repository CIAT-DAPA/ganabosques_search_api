import re
from fastapi import Query, HTTPException
from typing import Optional, List, Dict, Literal, Union
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.movement import Movement
from ganabosques_orm.collections.farmpolygons import FarmPolygons
from ganabosques_orm.collections.enterprise import Enterprise
from ganabosques_orm.enums.species import Species
from ganabosques_orm.enums.typemovement import TypeMovement
from mongoengine.queryset.visitor import Q
import time
from collections import defaultdict
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
    destination: Union[FarmSchema, EnterpriseSchema]

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


def process_movements_python(movements, direction, farm_id):
    """
    Process movements and generate statistics using pure Python.
    
    This function iterates through movement records and accumulates statistics 
    including animal counts, movement counts, and associated farms/enterprises. 
    It also performs lookups to retrieve complete details for related entities.
    
    Args:
        movements: QuerySet of Movement documents
        direction: Direction of movement - "in" (incoming) or "out" (outgoing)
        farm_id: ObjectId of the farm being analyzed
    
    Returns:
        Dictionary containing farms list, enterprises list, and organized statistics
    """
    # Estructuras para acumular datos
    stats_by_year = defaultdict(lambda: {
        "species": defaultdict(lambda: defaultdict(lambda: {"headcount": 0, "movements": 0})),
        "farms": set(),
        "enterprises": set()
    })
    
    # Contador de movimientos por destino (para farms y enterprises)
    farm_movement_counts = defaultdict(lambda: {"count": 0, "type": None})
    enterprise_movement_counts = defaultdict(lambda: {"count": 0, "type": None})
    
    for mov in movements:
        if not mov.date:
            continue
            
        year = str(mov.date.year)
        species = str(mov.species.value) if mov.species else "unknown"
        
        # Procesar clasificaciones
        for classification in (mov.movement or []):
            label = classification.label if classification.label else "unknown"
            amount = classification.amount if classification.amount else 0
            
            stats_by_year[year]["species"][species][label]["headcount"] += amount
            stats_by_year[year]["species"][species][label]["movements"] += 1
        
        # Agregar farms/enterprises según dirección
        if direction == "in":
            # Entradas: origen es lo que nos interesa
            if mov.farm_id_origin:
                farm_id_str = str(mov.farm_id_origin.id)
                stats_by_year[year]["farms"].add(farm_id_str)
                farm_movement_counts[farm_id_str]["count"] += 1
                farm_movement_counts[farm_id_str]["type"] = str(mov.type_origin.value) if mov.type_origin else "FARM"
            
            if mov.enterprise_id_origin:
                ent_id_str = str(mov.enterprise_id_origin.id)
                stats_by_year[year]["enterprises"].add(ent_id_str)
                enterprise_movement_counts[ent_id_str]["count"] += 1
                enterprise_movement_counts[ent_id_str]["type"] = str(mov.type_origin.value) if mov.type_origin else "UNKNOWN"
        else:
            # Salidas: destino es lo que nos interesa
            if mov.farm_id_destination:
                farm_id_str = str(mov.farm_id_destination.id)
                stats_by_year[year]["farms"].add(farm_id_str)
                farm_movement_counts[farm_id_str]["count"] += 1
                farm_movement_counts[farm_id_str]["type"] = str(mov.type_destination.value) if mov.type_destination else "FARM"
            
            if mov.enterprise_id_destination:
                ent_id_str = str(mov.enterprise_id_destination.id)
                stats_by_year[year]["enterprises"].add(ent_id_str)
                enterprise_movement_counts[ent_id_str]["count"] += 1
                enterprise_movement_counts[ent_id_str]["type"] = str(mov.type_destination.value) if mov.type_destination else "UNKNOWN"
    
    # Convertir sets a listas para las estadísticas
    statistics = {}
    for year, data in stats_by_year.items():
        statistics[year] = {
            "species": {
                sp: dict(labels) for sp, labels in data["species"].items()
            },
            "farms": list(data["farms"]),
            "enterprises": list(data["enterprises"])
        }
    
    # Hacer lookups de farms y enterprises para obtener detalles completos
    farms_list = []
    enterprises_list = []
    
    # Lookup farms
    if farm_movement_counts:
        farm_ids = [ObjectId(fid) for fid in farm_movement_counts.keys()]
        farmpolygons = FarmPolygons.objects(farm_id__in=farm_ids)
        
        farmpolygons_dict = {str(fp.farm_id.id): fp for fp in farmpolygons if fp.farm_id}
        
        for farm_id_str, mov_data in farm_movement_counts.items():
            if farm_id_str in farmpolygons_dict:
                fp = farmpolygons_dict[farm_id_str]
                farms_list.append({
                    "movements": mov_data["count"],
                    "direction": direction,
                    "destination_type": mov_data["type"],
                    "destination": convert_object_ids(fp.to_mongo().to_dict())
                })
    
    # Lookup enterprises
    if enterprise_movement_counts:
        ent_ids = [ObjectId(eid) for eid in enterprise_movement_counts.keys()]
        enterprises = Enterprise.objects(id__in=ent_ids)
        
        enterprises_dict = {str(ent.id): ent for ent in enterprises}
        
        for ent_id_str, mov_data in enterprise_movement_counts.items():
            if ent_id_str in enterprises_dict:
                ent = enterprises_dict[ent_id_str]
                enterprises_list.append({
                    "movements": mov_data["count"],
                    "direction": direction,
                    "destination_type": mov_data["type"],
                    "destination": convert_object_ids(ent.to_mongo().to_dict())
                })
    
    return {
        "farms": farms_list,
        "enterprises": enterprises_list,
        "statistics": statistics
    }


def calculate_mixed_python(inputs_stats, outputs_stats):
    """
    Calculate bidirectional movements (mixed) between inputs and outputs.
    
    This function identifies farms and enterprises that have both incoming and outgoing 
    movements within the same year, creating a set intersection for each year.
    
    Args:
        inputs_stats: Dictionary with input statistics organized by year
        outputs_stats: Dictionary with output statistics organized by year
    
    Returns:
        Dictionary with years as keys and farms/enterprises with bidirectional movements as values
    """
    mixed = {}
    
    # Obtener todos los años que aparecen en ambos
    all_years = set(inputs_stats.keys()) | set(outputs_stats.keys())
    
    for year in all_years:
        input_year = inputs_stats.get(year, {"farms": [], "enterprises": []})
        output_year = outputs_stats.get(year, {"farms": [], "enterprises": []})
        
        # Intersección de farms
        input_farms = set(input_year.get("farms", []))
        output_farms = set(output_year.get("farms", []))
        mixed_farms = list(input_farms & output_farms)
        
        # Intersección de enterprises
        input_enterprises = set(input_year.get("enterprises", []))
        output_enterprises = set(output_year.get("enterprises", []))
        mixed_enterprises = list(input_enterprises & output_enterprises)
        
        mixed[year] = {
            "farms": mixed_farms,
            "enterprises": mixed_enterprises
        }
    
    return mixed


def calculate_statistics_python_pure(farm_id):
    """
    Calculate movement statistics using pure Python processing.
    
    This implementation retrieves movements from MongoDB and processes all statistics 
    in Python, providing better code readability and maintainability compared to 
    complex MongoDB aggregation pipelines.
    
    Args:
        farm_id: ObjectId of the farm to analyze
    
    Returns:
        Dictionary containing inputs, outputs, and mixed movement statistics
    """
    # Query movimientos de entrada y salida
    movements_in = Movement.objects(farm_id_destination=farm_id)
    movements_out = Movement.objects(farm_id_origin=farm_id)
    
    # Procesar en Python
    inputs = process_movements_python(movements_in, "in", farm_id)
    outputs = process_movements_python(movements_out, "out", farm_id)
    
    # Calcular mixed
    mixed = calculate_mixed_python(
        inputs.get("statistics", {}),
        outputs.get("statistics", {})
    )
    
    return {
        "inputs": inputs,
        "outputs": outputs,
        "mixed": mixed
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

def get_movement_by_farmid_grouped(
    ids: str = Query(..., description="One or more farm_ids separated by commas to filter movement records"),
):
    """
    Retrieve aggregated movement statistics for one or more farms.
    
    This endpoint uses MongoDB aggregation pipelines to efficiently compute movement 
    statistics including inputs, outputs, and bidirectional movements (mixed) for 
    the specified farm IDs. Statistics are organized by year, species, and category.
    
    Returns detailed information about:
    - Input movements (animals coming into the farm)
    - Output movements (animals leaving the farm)
    - Mixed movements (farms/enterprises with bidirectional traffic)
    - Categorized statistics by species and classification labels
    """
    import time

    t0 = time.perf_counter()
    farm_ids = [ObjectId(i) for i in parse_object_ids(ids)]
    t1 = time.perf_counter()

    results_by_farm = {}

    for farm_id in farm_ids:
        pipeline = [
    {
        "$match": {
            "$or": [
                {"farm_id_origin": farm_id},
                {"farm_id_destination": farm_id}
            ]
        }
    },
    {
        "$facet": {
            # === Salidas hacia granjas ===
            "farms_out": [
                {"$match": {"farm_id_origin": farm_id, "type_destination": "FARM"}},
                {"$group": {
                    "_id": "$farm_id_destination",
                    "movements": {"$sum": 1},
                    "type_destination": {"$first": "$type_destination"}
                }},
                {"$lookup": {
                    "from": "farmpolygons",
                    "localField": "_id",
                    "foreignField": "farm_id",
                    "as": "destination_info"
                }},
                {"$unwind": "$destination_info"},
                {"$project": {
                    "_id": 0,
                    "direction": "out",
                    "destination_type": "$type_destination",
                    "movements": 1,
                    "destination": "$destination_info"
                }}
            ],
            # === Entradas desde granjas ===
            "farms_in": [
                {"$match": {"farm_id_destination": farm_id, "type_origin": "FARM"}},
                {"$group": {
                    "_id": "$farm_id_origin",
                    "movements": {"$sum": 1},
                    "type_destination": {"$first": "$type_origin"}
                }},
                {"$lookup": {
                    "from": "farmpolygons",
                    "localField": "_id",
                    "foreignField": "farm_id",
                    "as": "destination_info"
                }},
                {"$unwind": "$destination_info"},
                {"$project": {
                    "_id": 0,
                    "direction": "in",
                    "destination_type": "$type_destination",
                    "movements": 1,
                    "destination": "$destination_info"
                }}
            ],
            # === Salidas hacia empresas ===
            "enterprises_out": [
                {"$match": {"farm_id_origin": farm_id, "type_destination": {"$ne": "FARM"}}},
                {"$group": {
                    "_id": "$enterprise_id_destination",
                    "movements": {"$sum": 1},
                    "type_destination": {"$first": "$type_destination"}
                }},
                {"$lookup": {
                    "from": "enterprise",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "destination_info"
                }},
                {"$unwind": "$destination_info"},
                {"$project": {
                    "_id": 0,
                    "direction": "out",
                    "destination_type": "$type_destination",
                    "movements": 1,
                    "destination": "$destination_info"
                }}
            ],
            # === Entradas desde empresas ===
            "enterprises_in": [
                {"$match": {"farm_id_destination": farm_id, "type_origin": {"$ne": "FARM"}}},
                {"$group": {
                    "_id": "$enterprise_id_origin",
                    "movements": {"$sum": 1},
                    "type_destination": {"$first": "$type_origin"}
                }},
                {"$lookup": {
                    "from": "enterprise",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "destination_info"
                }},
                {"$unwind": "$destination_info"},
                {"$project": {
                    "_id": 0,
                    "direction": "in",
                    "destination_type": "$type_destination",
                    "movements": 1,
                    "destination": "$destination_info"
                }}
            ],
            # === Estadísticas salidas ===
            "statistic_out": [
                {"$match": {"farm_id_origin": farm_id}},
                {"$unwind": "$movement"},
                {"$group": {
                    "_id": {
                        "year": {"$year": "$date"},
                        "species": "$species",
                        "label": "$movement.label"
                    },
                    "headcount": {"$sum": "$movement.amount"},
                    "movements": {"$sum": 1},
                    "farms": {"$addToSet": "$farm_id_destination"},
                    "enterprises": {"$addToSet": "$enterprise_id_destination"}
                }},
                {"$group": {
                    "_id": {"year": "$_id.year", "species": "$_id.species"},
                    "labels": {"$push": {
                        "k": "$_id.label",
                        "v": {"headcount": "$headcount", "movements": "$movements"}
                    }},
                    "farms": {"$push": "$farms"},
                    "enterprises": {"$push": "$enterprises"}
                }},
                {"$project": {
                    "_id": "$_id",
                    "labels": "$labels",
                    "farms": {
                        "$reduce": {
                            "input": "$farms",
                            "initialValue": [],
                            "in": {"$setUnion": ["$$value", "$$this"]}
                        }
                    },
                    "enterprises": {
                        "$reduce": {
                            "input": "$enterprises",
                            "initialValue": [],
                            "in": {"$setUnion": ["$$value", "$$this"]}
                        }
                    }
                }},
                {"$group": {
                    "_id": "$_id.year",
                    "species": {"$push": {"k": "$_id.species", "v": {"$arrayToObject": "$labels"}}},
                    "farms": {"$first": "$farms"},
                    "enterprises": {"$first": "$enterprises"}
                }},
                {"$project": {
                    "k": {"$toString": "$_id"},
                    "v": {
                        "species": {"$arrayToObject": "$species"},
                        "farms": "$farms",
                        "enterprises": "$enterprises"
                    }
                }},
                {"$replaceRoot": {"newRoot": {"k": "$k", "v": "$v"}}},
                {"$group": {"_id": None, "statistics": {"$push": {"k": "$k", "v": "$v"}}}},
                {"$project": {"_id": 0, "statistics": {"$arrayToObject": "$statistics"}}}
            ],
            # === Estadísticas entradas ===
            "statistic_in": [
                {"$match": {"farm_id_destination": farm_id}},
                {"$unwind": "$movement"},
                {"$group": {
                    "_id": {
                        "year": {"$year": "$date"},
                        "species": "$species",
                        "label": "$movement.label"
                    },
                    "headcount": {"$sum": "$movement.amount"},
                    "movements": {"$sum": 1},
                    "farms": {"$addToSet": "$farm_id_origin"},
                    "enterprises": {"$addToSet": "$enterprise_id_origin"}
                }},
                {"$group": {
                    "_id": {"year": "$_id.year", "species": "$_id.species"},
                    "labels": {"$push": {
                        "k": "$_id.label",
                        "v": {"headcount": "$headcount", "movements": "$movements"}
                    }},
                    "farms": {"$push": "$farms"},
                    "enterprises": {"$push": "$enterprises"}
                }},
                {"$project": {
                    "_id": "$_id",
                    "labels": "$labels",
                    "farms": {
                        "$reduce": {
                            "input": "$farms",
                            "initialValue": [],
                            "in": {"$setUnion": ["$$value", "$$this"]}
                        }
                    },
                    "enterprises": {
                        "$reduce": {
                            "input": "$enterprises",
                            "initialValue": [],
                            "in": {"$setUnion": ["$$value", "$$this"]}
                        }
                    }
                }},
                {"$group": {
                    "_id": "$_id.year",
                    "species": {"$push": {"k": "$_id.species", "v": {"$arrayToObject": "$labels"}}},
                    "farms": {"$first": "$farms"},
                    "enterprises": {"$first": "$enterprises"}
                }},
                {"$project": {
                    "k": {"$toString": "$_id"},
                    "v": {
                        "species": {"$arrayToObject": "$species"},
                        "farms": "$farms",
                        "enterprises": "$enterprises"
                    }
                }},
                {"$replaceRoot": {"newRoot": {"k": "$k", "v": "$v"}}},
                {"$group": {"_id": None, "statistics": {"$push": {"k": "$k", "v": "$v"}}}},
                {"$project": {"_id": 0, "statistics": {"$arrayToObject": "$statistics"}}}
            ]
        }
    },
    {
        "$project": {
            "_id": 0,
            "inputs": {
                "farms": "$farms_in",
                "enterprises": "$enterprises_in",
                "statistics": {"$arrayElemAt": ["$statistic_in.statistics", 0]}
            },
            "outputs": {
                "farms": "$farms_out",
                "enterprises": "$enterprises_out",
                "statistics": {"$arrayElemAt": ["$statistic_out.statistics", 0]}
            },
            "mixed": {
                "$arrayToObject": {
                    "$map": {
                        "input": {"$objectToArray": {"$arrayElemAt": ["$statistic_in.statistics", 0]}},
                        "as": "inStat",
                        "in": {
                            "k": "$$inStat.k",
                            "v": {
                                "farms": {
                                    "$setIntersection": [
                                        "$$inStat.v.farms",
                                        {"$getField": {
                                            "field": "farms",
                                            "input": {"$getField": {"field": "$$inStat.k", "input": {"$arrayElemAt": ["$statistic_out.statistics", 0]}}}
                                        }}
                                    ]
                                },
                                "enterprises": {
                                    "$setIntersection": [
                                        "$$inStat.v.enterprises",
                                        {"$getField": {
                                            "field": "enterprises",
                                            "input": {"$getField": {"field": "$$inStat.k", "input": {"$arrayElemAt": ["$statistic_out.statistics", 0]}}}
                                        }}
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }
    }
]

        matches = list(Movement.objects.aggregate(pipeline))
        result = convert_object_ids(matches[0] if matches else {"inputs": {}, "outputs": {}})
        results_by_farm[str(farm_id)] = result

    t2 = time.perf_counter()

    logger.info(f"Grouped query for {len(farm_ids)} farm_ids executed in {(t2 - t1)*1000:.2f}ms")

    return results_by_farm

@router.get("/statistics-by-farmid")
def get_movement_statistics_python_pure(
    ids: str = Query(..., description="One or more farm_ids separated by commas to filter movement records"),
):
    """
    Retrieve movement statistics using Python-based processing.
    
    Alternative implementation that processes movement statistics entirely in Python 
    rather than using MongoDB aggregation pipelines. This approach offers improved 
    code readability and easier maintenance while providing the same statistical results.
    
    Recommended for scenarios where code clarity is prioritized over query performance.
    """
    t0 = time.perf_counter()
    farm_ids = [ObjectId(i) for i in parse_object_ids(ids)]
    t1 = time.perf_counter()

    results_by_farm = {}

    for farm_id in farm_ids:
        result = calculate_statistics_python_pure(farm_id)
        results_by_farm[str(farm_id)] = result

    t2 = time.perf_counter()

    logger.info(f"[PYTHON PURE] Query for {len(farm_ids)} farm_ids executed in {(t2 - t1)*1000:.2f}ms")

    return results_by_farm

@router.get("/statistics-by-enterpriseid")
def get_movement_by_farmidtest(
    ids: str = Query(..., description="One enterprise_id to filter movements records"),
):
    """
    Retrieve aggregated movement statistics for an enterprise.
    
    Computes comprehensive movement statistics for a specific enterprise including 
    inputs, outputs, and associated farms. Statistics are organized by year, species, 
    and classification categories.
    """

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
    """
    Recursively convert ObjectId instances to strings in nested data structures.
    
    This utility function traverses dictionaries, lists, and nested structures to 
    convert all MongoDB ObjectId instances to their string representation, making 
    the data JSON-serializable.
    
    Args:
        obj: Data structure to convert (dict, list, ObjectId, or primitive)
    
    Returns:
        Converted data structure with ObjectIds replaced by strings
    """
    if isinstance(obj, dict):
        return {k: convert_object_ids(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_object_ids(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj