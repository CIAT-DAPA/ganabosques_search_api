# routes/farmrisk_by_analysis_id_page.py

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from bson import ObjectId, DBRef

from ganabosques_orm.collections.farm import Farm
from ganabosques_orm.collections.farmrisk import FarmRisk
from ganabosques_orm.collections.farmpolygons import FarmPolygons
from ganabosques_orm.collections.adm3 import Adm3

from dependencies.auth_guard import require_admin

router = APIRouter(
    tags=["Farm Risk"],
    dependencies=[Depends(require_admin)]
)

PAGE_SIZE_DEFAULT = 20
PAGE_SIZE_MAX = 200


# ----------------------------
# Models
# ----------------------------

class RiskAreaItem(BaseModel):
    ha: float = 0.0
    prop: float = 0.0


class FarmInfo(BaseModel):
    farm_id: str

    adm3_id: Optional[str] = None
    adm3_name: Optional[str] = None

    # ext_id tal cual (array de objects)
    ext_id: Optional[List[dict]] = None

    enable: Optional[bool] = None
    created: Optional[str] = None
    updated: Optional[str] = None

    # desde FarmPolygons
    latitude: Optional[float] = None
    longitud: Optional[float] = None
    geojson: Optional[dict] = None   # ✅ geojson tal cual


class FarmRiskItem(BaseModel):
    _id: str
    analysis_id: str
    farm_id: str
    farm_polygons_id: Optional[str] = None

    risk_direct: bool
    risk_input: bool
    risk_output: bool

    deforestation: Optional[RiskAreaItem] = None
    farming_in: Optional[RiskAreaItem] = None
    farming_out: Optional[RiskAreaItem] = None
    protected: Optional[RiskAreaItem] = None

    farm: Optional[FarmInfo] = None


class PageResponse(BaseModel):
    page: int
    page_size: int
    items: List[FarmRiskItem]


# ----------------------------
# Helpers
# ----------------------------

def _as_object_id(val):
    if val is None:
        return None
    if isinstance(val, ObjectId):
        return val
    if isinstance(val, DBRef):
        return val.id
    if isinstance(val, dict) and "$id" in val:
        inner = val["$id"]
        return inner if isinstance(inner, ObjectId) else (ObjectId(inner) if ObjectId.is_valid(str(inner)) else None)
    s = str(val)
    return ObjectId(s) if ObjectId.is_valid(s) else None


def _iso(dt):
    try:
        return dt.isoformat() if dt else None
    except Exception:
        return None


def _area(obj) -> Optional[RiskAreaItem]:
    if not isinstance(obj, dict):
        return None
    return RiskAreaItem(
        ha=float(obj.get("ha") or 0.0),
        prop=float(obj.get("prop") or 0.0),
    )


# ----------------------------
# Route
# ----------------------------

@router.get("/farmrisk/by-analysis-id", response_model=PageResponse)
def get_farmrisk_by_analysis_id_page(
    analysis_id: str = Query(..., description="ObjectId del Analysis"),
    page: int = Query(1, ge=1, description="Página (1=primeros 20, 2=21-40, etc.)"),
    page_size: int = Query(PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
):
    """
    Retorna FarmRisk para un analysis_id con paginación por página (skip/limit).
    Adjunta:
      - Farm (incluye ext_id completo)
      - FarmPolygons latitude/longitude/geojson (buscado por farm_id)
      - Adm3 name (buscado por adm3_id del Farm)
    """
    try:
        analysis_oid = _as_object_id(analysis_id)
        if not analysis_oid:
            raise HTTPException(status_code=400, detail=f"Invalid ObjectId: {analysis_id}")

        skip = (page - 1) * page_size

        # 1) FarmRisk page
        coll_risk = FarmRisk._get_collection()
        risk_docs = list(
            coll_risk.find({"analysis_id": analysis_oid})
                    .sort([("_id", 1)])   # orden estable
                    .skip(skip)
                    .limit(page_size)
        )

        if not risk_docs:
            return PageResponse(page=page, page_size=page_size, items=[])

        # 2) Batch farms (por farm_id)
        farm_oids: List[ObjectId] = []
        for d in risk_docs:
            fid = _as_object_id(d.get("farm_id"))
            if fid:
                farm_oids.append(fid)
        farm_oids = list({x for x in farm_oids})

        farms = list(Farm.objects(id__in=farm_oids).no_dereference())

        # 3) farm_map base + recolectar adm3_ids
        farm_map: Dict[str, FarmInfo] = {}
        adm3_oids: List[ObjectId] = []

        for f in farms:
            fm = f.to_mongo().to_dict()

            adm3_oid = _as_object_id(fm.get("adm3_id"))
            if adm3_oid:
                adm3_oids.append(adm3_oid)

            log = fm.get("log") or {}

            farm_map[str(fm["_id"])] = FarmInfo(
                farm_id=str(fm["_id"]),
                adm3_id=str(adm3_oid) if adm3_oid else None,
                adm3_name=None,
                ext_id=fm.get("ext_id") or None,  # ✅ tal cual
                enable=bool(log.get("enable")) if "enable" in log else None,
                created=_iso(fm.get("created")),
                updated=_iso(fm.get("updated")),
                latitude=None,
                longitud=None,
                geojson=None,
            )

        adm3_oids = list({x for x in adm3_oids})

        # 4) Batch FarmPolygons por farm_id -> latitude/longitude/geojson (tal cual)
        if farm_oids:
            coll_poly = FarmPolygons._get_collection()
            poly_docs = list(
                coll_poly.find(
                    {"farm_id": {"$in": farm_oids}},
                    projection={
                        "farm_id": 1,
                        "latitude": 1,
                        "longitud": 1,
                        "geojson": 1,   # ✅ agregar geojson
                    }
                )
            )

            # Si hay varios polygons por farm, toma el primero
            seen = set()
            for p in poly_docs:
                fid = _as_object_id(p.get("farm_id"))
                if not fid:
                    continue
                fid_str = str(fid)
                if fid_str in seen:
                    continue
                seen.add(fid_str)

                if fid_str in farm_map:
                    farm_map[fid_str].latitude = p.get("latitude")
                    farm_map[fid_str].longitud = p.get("longitud")
                    farm_map[fid_str].geojson = p.get("geojson")  # ✅ tal cual

        # 5) Batch Adm3 name por adm3_id
        if adm3_oids:
            adm3_docs = list(
                Adm3.objects(id__in=adm3_oids)
                .no_dereference()
                .only("id", "name")
            )
            adm3_name_map = {str(a.id): (getattr(a, "name", None) or None) for a in adm3_docs}

            for finfo in farm_map.values():
                if finfo.adm3_id and finfo.adm3_id in adm3_name_map:
                    finfo.adm3_name = adm3_name_map[finfo.adm3_id]

        # 6) Response
        items: List[FarmRiskItem] = []
        for d in risk_docs:
            farm_id_oid = _as_object_id(d.get("farm_id"))
            farm_id_str = str(farm_id_oid) if farm_id_oid else str(d.get("farm_id"))

            farm_polygons_id = d.get("farm_polygons_id")
            farm_polygons_id = str(_as_object_id(farm_polygons_id)) if farm_polygons_id else None

            items.append(
                FarmRiskItem(
                    _id=str(d.get("_id")),
                    analysis_id=str(analysis_oid),
                    farm_id=farm_id_str,
                    farm_polygons_id=farm_polygons_id,
                    risk_direct=bool(d.get("risk_direct")),
                    risk_input=bool(d.get("risk_input")),
                    risk_output=bool(d.get("risk_output")),
                    deforestation=_area(d.get("deforestation")),
                    farming_in=_area(d.get("farming_in")),
                    farming_out=_area(d.get("farming_out")),
                    protected=_area(d.get("protected")),
                    farm=farm_map.get(farm_id_str),
                )
            )

        return PageResponse(page=page, page_size=page_size, items=items)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")