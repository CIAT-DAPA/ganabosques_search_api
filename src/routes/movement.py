import re
from fastapi import Query, HTTPException, Depends, APIRouter
from typing import Optional, List, Dict, Literal, Union
from pydantic import BaseModel, Field
from bson import ObjectId
from datetime import datetime
from ganabosques_orm.collections.movement import Movement
from ganabosques_orm.collections.farmpolygons import FarmPolygons
from ganabosques_orm.collections.farm import Farm
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
from dependencies.auth_guard import require_admin


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


class CategoryStatistics(BaseModel):
    headcount: int
    movements: int


SpeciesStatistics = Dict[str, CategoryStatistics]
YearStatistics = Dict[str, SpeciesStatistics]


class MovementDestination(BaseModel):
    direction: Literal["in", "out"]
    destination_type: str
    movements: int
    destination: Union[FarmSchema, EnterpriseSchema]


class MovementGroup(BaseModel):
    farms: List[MovementDestination]
    enterprises: List[MovementDestination]
    statistics: Optional[YearStatistics] = None


class MovementStatisticsResponse(BaseModel):
    inputs: MovementGroup
    outputs: MovementGroup


def serialize_movement(doc):
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
    stats = {
        "species": defaultdict(lambda: defaultdict(lambda: {"headcount": 0, "movements": 0})),
        "farms": set(),
        "enterprises": set()
    }
    farm_movement_counts = defaultdict(lambda: {"count": 0, "type": None})
    enterprise_movement_counts = defaultdict(lambda: {"count": 0, "type": None})
    movements_by_type = defaultdict(int)
    total_movements = 0
    
    for mov in movements:
        total_movements += 1
        
        # Contar por tipo de movimiento
        if direction == "in":
            mov_type = str(mov.type_origin.value) if mov.type_origin else "UNKNOWN"
        else:
            mov_type = str(mov.type_destination.value) if mov.type_destination else "UNKNOWN"
        movements_by_type[mov_type] += 1
        
        if not mov.date:
            continue
        species = str(mov.species.value) if mov.species else "unknown"
        for classification in (mov.movement or []):
            label = classification.label if classification.label else "unknown"
            amount = classification.amount if classification.amount else 0
            stats["species"][species][label]["headcount"] += amount
            stats["species"][species][label]["movements"] += 1
        if direction == "in":
            if mov.farm_id_origin:
                farm_id_str = str(mov.farm_id_origin.id)
                stats["farms"].add(farm_id_str)
                farm_movement_counts[farm_id_str]["count"] += 1
                farm_movement_counts[farm_id_str]["type"] = str(mov.type_origin.value) if mov.type_origin else "FARM"
            if mov.enterprise_id_origin:
                ent_id_str = str(mov.enterprise_id_origin.id)
                stats["enterprises"].add(ent_id_str)
                enterprise_movement_counts[ent_id_str]["count"] += 1
                enterprise_movement_counts[ent_id_str]["type"] = str(mov.type_origin.value) if mov.type_origin else "UNKNOWN"
        else:
            if mov.farm_id_destination:
                farm_id_str = str(mov.farm_id_destination.id)
                stats["farms"].add(farm_id_str)
                farm_movement_counts[farm_id_str]["count"] += 1
                farm_movement_counts[farm_id_str]["type"] = str(mov.type_destination.value) if mov.type_destination else "FARM"
            if mov.enterprise_id_destination:
                ent_id_str = str(mov.enterprise_id_destination.id)
                stats["enterprises"].add(ent_id_str)
                enterprise_movement_counts[ent_id_str]["count"] += 1
                enterprise_movement_counts[ent_id_str]["type"] = str(mov.type_destination.value) if mov.type_destination else "UNKNOWN"
    statistics = {
        "species": {
            sp: dict(labels) for sp, labels in stats["species"].items()
        },
        "farms": list(stats["farms"]),
        "enterprises": list(stats["enterprises"])
    }
    farms_list = []
    enterprises_list = []
    if farm_movement_counts:
        farm_ids = [ObjectId(fid) for fid in farm_movement_counts.keys()]
        farmpolygons = FarmPolygons.objects(farm_id__in=farm_ids).only('farm_id', 'latitude', 'longitud')
        farmpolygons_dict = {str(fp.farm_id.id): fp for fp in farmpolygons if fp.farm_id}
        
        farms = Farm.objects(id__in=farm_ids).only('id', 'ext_id')
        farms_dict = {str(f.id): f for f in farms}
        
        for farm_id_str, mov_data in farm_movement_counts.items():
            if farm_id_str in farmpolygons_dict:
                fp = farmpolygons_dict[farm_id_str]
                farm_obj = farms_dict.get(farm_id_str)
                
                ext_id_data = []
                if farm_obj and farm_obj.ext_id:
                    ext_id_data = [convert_object_ids(ext.to_mongo().to_dict()) for ext in farm_obj.ext_id]
                
                destination = {
                    "latitude": fp.latitude,
                    "longitud": fp.longitud,
                    "ext_id": ext_id_data,
                    "farm_id": str(fp.farm_id.id) if fp.farm_id else None
                }
                
                farms_list.append({
                    "movements": mov_data["count"],
                    "direction": direction,
                    "destination_type": mov_data["type"],
                    "destination": destination
                })
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
        "statistics": statistics,
        "movements_by_type": dict(movements_by_type),
        "total_movements": total_movements
    }


def calculate_summary(inputs_data, outputs_data):
    """Calcula resumen estadístico con porcentajes de movimientos"""
    total_inputs = inputs_data.get("total_movements", 0)
    total_outputs = outputs_data.get("total_movements", 0)
    total_movements = total_inputs + total_outputs
    
    def calculate_type_percentages(movements_by_type, direction_total, grand_total):
        """Calcula porcentajes para cada tipo"""
        result = {}
        for mov_type, count in movements_by_type.items():
            result[mov_type] = {
                "count": count,
                "percentage_of_inputs" if "inputs" in str(direction_total) else "percentage_of_outputs": round((count / direction_total * 100), 2) if direction_total > 0 else 0,
                "percentage_of_total": round((count / grand_total * 100), 2) if grand_total > 0 else 0
            }
        return result
    
    # Calcular porcentajes para inputs
    inputs_by_type = {}
    for mov_type, count in inputs_data.get("movements_by_type", {}).items():
        inputs_by_type[mov_type] = {
            "count": count,
            "percentage_of_inputs": round((count / total_inputs * 100), 2) if total_inputs > 0 else 0,
            "percentage_of_total": round((count / total_movements * 100), 2) if total_movements > 0 else 0
        }
    
    # Calcular porcentajes para outputs
    outputs_by_type = {}
    for mov_type, count in outputs_data.get("movements_by_type", {}).items():
        outputs_by_type[mov_type] = {
            "count": count,
            "percentage_of_outputs": round((count / total_outputs * 100), 2) if total_outputs > 0 else 0,
            "percentage_of_total": round((count / total_movements * 100), 2) if total_movements > 0 else 0
        }
    
    summary = {
        "total_movements": total_movements,
        "inputs": {
            "count": total_inputs,
            "percentage": round((total_inputs / total_movements * 100), 2) if total_movements > 0 else 0,
            "by_destination_type": inputs_by_type
        },
        "outputs": {
            "count": total_outputs,
            "percentage": round((total_outputs / total_movements * 100), 2) if total_movements > 0 else 0,
            "by_destination_type": outputs_by_type
        }
    }
    
    return summary


def process_movements_python_for_enterprise(movements, direction, enterprise_id):
    """Procesa movimientos desde la perspectiva de una enterprise"""
    stats = {
        "species": defaultdict(lambda: defaultdict(lambda: {"headcount": 0, "movements": 0})),
        "farms": set(),
        "enterprises": set()
    }
    farm_movement_counts = defaultdict(lambda: {"count": 0, "type": None})
    enterprise_movement_counts = defaultdict(lambda: {"count": 0, "type": None})
    movements_by_type = defaultdict(int)
    total_movements = 0
    
    for mov in movements:
        total_movements += 1
        
        # Contar por tipo de movimiento
        if direction == "in":
            mov_type = str(mov.type_origin.value) if mov.type_origin else "UNKNOWN"
        else:
            mov_type = str(mov.type_destination.value) if mov.type_destination else "UNKNOWN"
        movements_by_type[mov_type] += 1
        
        if not mov.date:
            continue
        species = str(mov.species.value) if mov.species else "unknown"
        for classification in (mov.movement or []):
            label = classification.label if classification.label else "unknown"
            amount = classification.amount if classification.amount else 0
            stats["species"][species][label]["headcount"] += amount
            stats["species"][species][label]["movements"] += 1
        if direction == "in":
            if mov.farm_id_origin:
                farm_id_str = str(mov.farm_id_origin.id)
                stats["farms"].add(farm_id_str)
                farm_movement_counts[farm_id_str]["count"] += 1
                farm_movement_counts[farm_id_str]["type"] = str(mov.type_origin.value) if mov.type_origin else "FARM"
            if mov.enterprise_id_origin:
                ent_id_str = str(mov.enterprise_id_origin.id)
                stats["enterprises"].add(ent_id_str)
                enterprise_movement_counts[ent_id_str]["count"] += 1
                enterprise_movement_counts[ent_id_str]["type"] = str(mov.type_origin.value) if mov.type_origin else "UNKNOWN"
        else:
            if mov.farm_id_destination:
                farm_id_str = str(mov.farm_id_destination.id)
                stats["farms"].add(farm_id_str)
                farm_movement_counts[farm_id_str]["count"] += 1
                farm_movement_counts[farm_id_str]["type"] = str(mov.type_destination.value) if mov.type_destination else "FARM"
            if mov.enterprise_id_destination:
                ent_id_str = str(mov.enterprise_id_destination.id)
                stats["enterprises"].add(ent_id_str)
                enterprise_movement_counts[ent_id_str]["count"] += 1
                enterprise_movement_counts[ent_id_str]["type"] = str(mov.type_destination.value) if mov.type_destination else "UNKNOWN"
    statistics = {
        "species": {
            sp: dict(labels) for sp, labels in stats["species"].items()
        },
        "farms": list(stats["farms"]),
        "enterprises": list(stats["enterprises"])
    }
    farms_list = []
    enterprises_list = []
    if farm_movement_counts:
        farm_ids = [ObjectId(fid) for fid in farm_movement_counts.keys()]
        farmpolygons = FarmPolygons.objects(farm_id__in=farm_ids).only('farm_id', 'latitude', 'longitud')
        farmpolygons_dict = {str(fp.farm_id.id): fp for fp in farmpolygons if fp.farm_id}
        
        farms = Farm.objects(id__in=farm_ids).only('id', 'ext_id')
        farms_dict = {str(f.id): f for f in farms}
        
        for farm_id_str, mov_data in farm_movement_counts.items():
            if farm_id_str in farmpolygons_dict:
                fp = farmpolygons_dict[farm_id_str]
                farm_obj = farms_dict.get(farm_id_str)
                
                ext_id_data = []
                if farm_obj and farm_obj.ext_id:
                    ext_id_data = [convert_object_ids(ext.to_mongo().to_dict()) for ext in farm_obj.ext_id]
                
                destination = {
                    "latitude": fp.latitude,
                    "longitud": fp.longitud,
                    "ext_id": ext_id_data,
                    "farm_id": str(fp.farm_id.id) if fp.farm_id else None
                }
                
                farms_list.append({
                    "movements": mov_data["count"],
                    "direction": direction,
                    "destination_type": mov_data["type"],
                    "destination": destination
                })
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
        "statistics": statistics,
        "movements_by_type": dict(movements_by_type),
        "total_movements": total_movements
    }


def calculate_statistics_python_pure_for_enterprise(enterprise_id, start_date, end_date):
    """Calcula estadísticas para una enterprise usando Python puro"""
    movements_in = Movement.objects(
        enterprise_id_destination=enterprise_id,
        date__gte=start_date,
        date__lte=end_date
    )
    movements_out = Movement.objects(
        enterprise_id_origin=enterprise_id,
        date__gte=start_date,
        date__lte=end_date
    )
    inputs = process_movements_python_for_enterprise(movements_in, "in", enterprise_id)
    outputs = process_movements_python_for_enterprise(movements_out, "out", enterprise_id)
    
    summary = calculate_summary(inputs, outputs)
    
    mixed = calculate_mixed_python(
        inputs.get("statistics", {}),
        outputs.get("statistics", {})
    )
    return {
        "summary": summary,
        "inputs": inputs,
        "outputs": outputs,
        "mixed": mixed
    }


def calculate_mixed_python(inputs_stats, outputs_stats):
    input_farms = set(inputs_stats.get("farms", []))
    output_farms = set(outputs_stats.get("farms", []))
    mixed_farms = list(input_farms & output_farms)
    input_enterprises = set(inputs_stats.get("enterprises", []))
    output_enterprises = set(outputs_stats.get("enterprises", []))
    mixed_enterprises = list(input_enterprises & output_enterprises)
    return {
        "farms": mixed_farms,
        "enterprises": mixed_enterprises
    }


def calculate_statistics_python_pure(farm_id, start_date, end_date):
    movements_in = Movement.objects(
        farm_id_destination=farm_id,
        date__gte=start_date,
        date__lte=end_date
    )
    movements_out = Movement.objects(
        farm_id_origin=farm_id,
        date__gte=start_date,
        date__lte=end_date
    )
    inputs = process_movements_python(movements_in, "in", farm_id)
    outputs = process_movements_python(movements_out, "out", farm_id)
    
    summary = calculate_summary(inputs, outputs)
    
    mixed = calculate_mixed_python(
        inputs.get("statistics", {}),
        outputs.get("statistics", {})
    )
    return {
        "summary": summary,
        "inputs": inputs,
        "outputs": outputs,
        "mixed": mixed
    }


_inner_router = generate_read_only_router(
    prefix="/movement",
    tags=["Movement"],
    collection=Movement,
    schema_model=MovementSchema,
    allowed_fields=["ext_id", "species", "type_origin", "type_destination"],
    serialize_fn=serialize_movement,
    include_endpoints=["paged", "by-extid"]
)


@_inner_router.get("/by-farmid", response_model=List[MovementSchema])
def get_movement_by_farmid(
    ids: str = Query(..., description="One or more comma-separated farm_id to filter movements records"),
    roles: Optional[str] = Query(
        None, description="Which role(s) to filter by: 'origin', 'destination', or both (default: both)",
    )
):
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


@_inner_router.get("/by-enterpriseid", response_model=List[MovementSchema])
def get_movement_by_enterpriseid(
    ids: str = Query(..., description="One or more comma-separated enterprise_id to filter movements records"),
    roles: Optional[str] = Query(
        None, description="Which role(s) to filter by: 'origin', 'destination', or both (default: both)",
    )
):
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


@_inner_router.get("/statistics-by-farmid")
def get_movement_statistics_python_pure(
    ids: str = Query(..., description="One or more farm_ids separated by commas to filter movement records"),
    start_date: str = Query(..., description="Fecha inicio (ISO format: YYYY-MM-DD)"),
    end_date: str = Query(..., description="Fecha fin (ISO format: YYYY-MM-DD)")
):
    t0 = time.perf_counter()
    try:
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
        if end < start:
            raise HTTPException(status_code=400, detail="end_date must be greater than or equal to start_date")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format. Use YYYY-MM-DD. Error: {str(e)}")
    
    farm_ids = [ObjectId(i) for i in parse_object_ids(ids)]
    t1 = time.perf_counter()
    results_by_farm = {}
    for farm_id in farm_ids:
        result = calculate_statistics_python_pure(farm_id, start, end)
        results_by_farm[str(farm_id)] = result
    t2 = time.perf_counter()
    logger.info(f"[PYTHON PURE] Query for {len(farm_ids)} farm_ids executed in {(t2 - t1)*1000:.2f}ms (date range: {start_date} to {end_date})")
    return results_by_farm


@_inner_router.get("/statistics-by-enterpriseid")
def get_movement_statistics_by_enterpriseid(
    ids: str = Query(..., description="One or more enterprise_ids separated by commas to filter movement records"),
    start_date: str = Query(..., description="Fecha inicio (ISO format: YYYY-MM-DD)"),
    end_date: str = Query(..., description="Fecha fin (ISO format: YYYY-MM-DD)")
):
    t0 = time.perf_counter()
    try:
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
        if end < start:
            raise HTTPException(status_code=400, detail="end_date must be greater than or equal to start_date")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format. Use YYYY-MM-DD. Error: {str(e)}")
    
    enterprise_ids = [ObjectId(i) for i in parse_object_ids(ids)]
    t1 = time.perf_counter()
    results_by_enterprise = {}
    for enterprise_id in enterprise_ids:
        result = calculate_statistics_python_pure_for_enterprise(enterprise_id, start, end)
        results_by_enterprise[str(enterprise_id)] = result
    t2 = time.perf_counter()
    logger.info(f"[PYTHON PURE] Query for {len(enterprise_ids)} enterprise_ids executed in {(t2 - t1)*1000:.2f}ms (date range: {start_date} to {end_date})")
    return results_by_enterprise


def convert_object_ids(obj):
    if isinstance(obj, dict):
        return {k: convert_object_ids(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_object_ids(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj


router = APIRouter(
    #dependencies=[Depends(require_admin)]
)

router.include_router(_inner_router)