import re
import json
from fastapi import Query, Depends, APIRouter
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

from ganabosques_orm.collections.suppliers import Suppliers
from ganabosques_orm.collections.farm import Farm  # ✅ NUEVO
from schemas.logschema import LogSchema

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids
from dependencies.auth_guard import require_admin


class SuppliersSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the supplier record")
    enterprise_id: Optional[str] = Field(None, description="MongoDB ID of the associated enterprise")
    farm_id: Optional[str] = Field(None, description="MongoDB ID of the associated farm")

    # ✅ ext_id del FARM asociado (lo que pediste)
    ext_id: Optional[Any] = Field(None, description="ext_id from the related Farm document")

    years: Optional[List[str]] = Field(None, description="List of years linked to this supplier relation")
    log: Optional[LogSchema] = Field(None, description="Logging information")

    class Config:
        from_attributes = True


def _normalize_years(raw_years) -> List[str]:
    """
    years puede venir como:
    - [2017, 2018] (ints)
    - ["2017", "2018"] (strings)
    - [{"years": "2017"}, {"years": 2018}] (legacy)
    -> siempre devolvemos List[str]
    """
    if raw_years is None:
        return []

    years_out: List[str] = []
    if isinstance(raw_years, list):
        for y in raw_years:
            if isinstance(y, dict) and "years" in y:
                years_out.append(str(y.get("years")))
            else:
                years_out.append(str(y))
    else:
        years_out = [str(raw_years)]

    return years_out


def serialize_supplier(doc, farm_ext_map: Optional[Dict[str, Any]] = None):
    farm_ext_map = farm_ext_map or {}

    farm_id_str = str(doc.farm_id.id) if getattr(doc, "farm_id", None) else None
    ext_id_val = farm_ext_map.get(farm_id_str) if farm_id_str else None

    return {
        "id": str(doc.id),
        "enterprise_id": str(doc.enterprise_id.id) if doc.enterprise_id else None,
        "farm_id": farm_id_str,
        "ext_id": ext_id_val,  # ✅ ext_id del farm
        "years": _normalize_years(getattr(doc, "years", None)),
        "log": {
            "enable": doc.log.enable if doc.log else None,
            "created": doc.log.created.isoformat() if doc.log and doc.log.created else None,
            "updated": doc.log.updated.isoformat() if doc.log and doc.log.updated else None
        } if doc.log else None
    }


def _build_farm_ext_map_from_suppliers(matches: List[Suppliers]) -> Dict[str, Any]:
    """
    Hace un lookup en Farm por todos los farm_id encontrados en matches,
    y devuelve un mapa: { "<farm_id>": farm.ext_id }
    """
    farm_ids = {
        supe.farm_id.id
        for supe in matches
        if getattr(supe, "farm_id", None) and getattr(supe.farm_id, "id", None)
    }

    if not farm_ids:
        return {}

    farms = list(
        Farm.objects(id__in=list(farm_ids))
        .no_dereference()
        .only("id", "ext_id")
    )

    farm_ext_map: Dict[str, Any] = {}
    for f in farms:
        fm = f.to_mongo().to_dict()
        farm_ext_map[str(fm.get("_id"))] = fm.get("ext_id")

    return farm_ext_map


_inner_router = generate_read_only_router(
    prefix="/suppliers",
    tags=["Farm and Enterprise"],
    collection=Suppliers,
    schema_model=SuppliersSchema,
    allowed_fields=[],
    serialize_fn=serialize_supplier,  # usado por endpoints generados (ej: paged)
    include_endpoints=["paged"]
)


# 🔥 BY FARM AGRUPADO + ext_id del Farm
@_inner_router.get("/by-farm", response_model=Dict[str, List[SuppliersSchema]])
def get_supplier_by_farm_ids_grouped(
    ids: str = Query(..., description="Comma-separated Farm IDs")
):
    search_ids = parse_object_ids(ids)
    search_id_strs = [str(x) for x in search_ids]

    matches = list(Suppliers.objects(farm_id__in=search_ids))

    # ✅ lookup ext_id en Farm (batch)
    farm_ext_map = _build_farm_ext_map_from_suppliers(matches)

    bucket: Dict[str, List[dict]] = {}
    for supe in matches:
        fid = str(supe.farm_id.id) if supe.farm_id else None
        if not fid:
            continue
        bucket.setdefault(fid, []).append(serialize_supplier(supe, farm_ext_map=farm_ext_map))

    # Mantiene el orden del query + incluye vacíos
    ordered_grouped = {fid: bucket.get(fid, []) for fid in search_id_strs}
    return ordered_grouped


# 🔥 BY ENTERPRISE AGRUPADO + ext_id del Farm (igual)
@_inner_router.get("/by-enterprise", response_model=Dict[str, List[SuppliersSchema]])
def get_supplier_by_enterprise_ids_grouped(
    ids: str = Query(..., description="Comma-separated Enterprise IDs")
):
    search_ids = parse_object_ids(ids)
    search_id_strs = [str(x) for x in search_ids]

    matches = list(Suppliers.objects(enterprise_id__in=search_ids))

    # ✅ lookup ext_id en Farm (batch)
    farm_ext_map = _build_farm_ext_map_from_suppliers(matches)

    bucket: Dict[str, List[dict]] = {}
    for supe in matches:
        eid = str(supe.enterprise_id.id) if supe.enterprise_id else None
        if not eid:
            continue
        bucket.setdefault(eid, []).append(serialize_supplier(supe, farm_ext_map=farm_ext_map))

    ordered_grouped = {eid: bucket.get(eid, []) for eid in search_id_strs}
    return ordered_grouped


router = APIRouter(
    dependencies=[Depends(require_admin)]
)

router.include_router(_inner_router)