# routes/adm3risk_by_adm3_and_type.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, RootModel
from typing import List, Dict, Literal, Optional
from bson import ObjectId
from bson.dbref import DBRef
import logging, traceback

# Ajusta estos imports si tus paths/modelos difieren
from ganabosques_orm.collections.deforestation import Deforestation
from ganabosques_orm.collections.analysis import Analysis
from ganabosques_orm.collections.adm3risk import Adm3Risk
from ganabosques_orm.collections.adm3 import Adm3

router = APIRouter()
MAX_IDS = 500

DEBUG = True 

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("adm3risk")

def dbg(msg: str):
    if DEBUG:
        print(f"[adm3risk] {msg}")
    log.info(msg)

def dump_exc(where: str, e: Exception):
    dbg(f"ERROR en {where}: {e.__class__.__name__}: {e}")

class RequestBody(BaseModel):
    adm3_ids: List[str] = Field(..., description="Lista de ObjectIds de ADM3")
    type: Literal["annual", "cumulative"]

class Adm3RiskItem(BaseModel):
    _id: str
    adm3_id: str
    analysis_id: str
    deforestation_id: str
    year_start: Optional[int] = None
    year_end: Optional[int] = None
    def_ha: Optional[float] = None
    farm_amount: Optional[int] = None
    risk_total: Optional[float] = None

class Adm3Group(BaseModel):
    adm3_id: str
    name: Optional[str] = None
    department: Optional[str] = None
    municipality: Optional[str] = None  # <-- NUEVO
    items: List[Adm3RiskItem] = Field(default_factory=list)

class Adm3RiskGroupedResponse(RootModel[Dict[str, Adm3Group]]):
    pass

def _validate_object_ids(ids: List[str], label: str = "ids") -> List[ObjectId]:
    if not ids:
        raise HTTPException(status_code=400, detail=f"'{label}' no puede estar vacío")
    if len(ids) > MAX_IDS:
        raise HTTPException(status_code=400, detail=f"'{label}' excede el máximo de {MAX_IDS} IDs")
    out: List[ObjectId] = []
    seen: set[str] = set()
    for raw in ids:
        if not ObjectId.is_valid(raw):
            raise HTTPException(status_code=400, detail=f"Invalid {label[:-1]}: {raw}")
        if raw not in seen:
            seen.add(raw)
            out.append(ObjectId(raw))
    return out

def _as_object_id(val) -> Optional[ObjectId]:
    """DBRef/ObjectId/dict{'$id':..}/string -> ObjectId | None"""
    if val is None:
        return None
    if isinstance(val, ObjectId):
        return val
    if isinstance(val, DBRef):
        return val.id
    if hasattr(val, "id"):
        return val.id
    if isinstance(val, dict) and "$id" in val:
        return val["$id"]
    s = str(val)
    return ObjectId(s) if ObjectId.is_valid(s) else None

def _split_label(label: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not label:
        return None, None, None
    parts = [p.strip() for p in str(label).split(",")]
    dep = parts[0] if len(parts) >= 1 else None
    mun = parts[1] if len(parts) >= 2 else None
    nm  = parts[2] if len(parts) >= 3 else None
    return dep, mun, nm

# ---------------- Endpoint ----------------
@router.post("/adm3risk/by-adm3-and-type", response_model=Adm3RiskGroupedResponse)
def get_adm3risk_by_adm3_and_type(payload: RequestBody):

    try:
        valid_adm3_ids = _validate_object_ids(payload.adm3_ids, "adm3_ids")
    except HTTPException:
        raise
    except Exception as e:
        dump_exc("validación", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    defo_type = payload.type

    try:
        qs_defo = (
            Deforestation.objects(deforestation_type=defo_type)
            .no_dereference()
            .only("id", "year_start", "year_end")
        )
        deforestations = list(qs_defo)
        if not deforestations:
            dbg("No hay deforestations; retorno {}")
            return Adm3RiskGroupedResponse(root={})

        defo_ids = [d.id for d in deforestations]
        defo_years: Dict[str, Dict[str, Optional[int]]] = {
            str(d.id): {"year_start": getattr(d, "year_start", None),
                        "year_end": getattr(d, "year_end", None)}
            for d in deforestations
        }
    except Exception as e:
        dump_exc("consulta Deforestation", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    try:
        qs_anal = (
            Analysis.objects(deforestation_id__in=defo_ids)
            .no_dereference()
            .only("id", "deforestation_id")
        )
        analyses = list(qs_anal)
        if not analyses:
            dbg("No hay analyses; retorno {}")
            return Adm3RiskGroupedResponse(root={})

        analysis_ids = [a.id for a in analyses]
        analysis_to_defo: Dict[str, str] = {}
        for a in analyses:
            oid = _as_object_id(getattr(a, "deforestation_id", None))
            if not oid:
                continue
            analysis_to_defo[str(a.id)] = str(oid)

        if analysis_to_defo:
            k, v = next(iter(analysis_to_defo.items()))
    except Exception as e:
        dump_exc("consulta Analysis", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    try:
        qs_risk = (
            Adm3Risk.objects(adm3_id__in=valid_adm3_ids, analysis_id__in=analysis_ids)
            .no_dereference()
            .only("id", "adm3_id", "analysis_id", "def_ha", "farm_amount", "risk_total")
            .order_by("adm3_id", "analysis_id")
        )
        risks = list(qs_risk)
    except Exception as e:
        dump_exc("consulta Adm3Risk", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    try:
        adm3_docs = list(
            Adm3.objects(id__in=valid_adm3_ids)
            .no_dereference()
            .only("id", "name", "label")
        )
        adm3_meta: Dict[str, Dict[str, Optional[str]]] = {}
        for d in adm3_docs:
            label = getattr(d, "label", None)
            dep, mun, _ = _split_label(label)
            adm3_meta[str(d.id)] = {
                "name": getattr(d, "name", None),
                "department": dep,
                "municipality": mun,
            }
        grouped: Dict[str, Adm3Group] = {
            str(oid): Adm3Group(
                adm3_id=str(oid),
                name=adm3_meta.get(str(oid), {}).get("name"),
                department=adm3_meta.get(str(oid), {}).get("department"),
                municipality=adm3_meta.get(str(oid), {}).get("municipality"),
                items=[]
            )
            for oid in valid_adm3_ids
        }
    except Exception as e:
        dump_exc("consulta ADM3", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    try:
        for r in risks:
            doc = r.to_mongo().to_dict()

            adm3_id_str = str(doc.get("adm3_id"))
            analysis_id_str = str(doc.get("analysis_id"))
            deforestation_id_str = analysis_to_defo.get(analysis_id_str)
            years = defo_years.get(deforestation_id_str, {"year_start": None, "year_end": None})

            item = Adm3RiskItem(
                _id=str(doc["_id"]),
                adm3_id=adm3_id_str,
                analysis_id=analysis_id_str,
                deforestation_id=deforestation_id_str or "",
                year_start=years.get("year_start"),
                year_end=years.get("year_end"),
                def_ha=doc.get("def_ha"),
                farm_amount=doc.get("farm_amount"),
                risk_total=doc.get("risk_total"),
            )

            if adm3_id_str not in grouped:
                grouped[adm3_id_str] = Adm3Group(
                    adm3_id=adm3_id_str,
                    name=adm3_meta.get(adm3_id_str, {}).get("name"),
                    department=adm3_meta.get(adm3_id_str, {}).get("department"),
                    municipality=adm3_meta.get(adm3_id_str, {}).get("municipality"),
                    items=[]
                )
            grouped[adm3_id_str].items.append(item)

        if grouped:
            fk = next(iter(grouped))

        return Adm3RiskGroupedResponse(root=grouped)

    except Exception as e:
        dump_exc("agrupación/serialización", e)
        raise HTTPException(status_code=500, detail="Internal server error")
