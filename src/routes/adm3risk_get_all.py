# routes/adm3risk_by_adm3_and_type.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, RootModel
from typing import List, Dict, Literal, Optional, Tuple
from bson import ObjectId
from bson.dbref import DBRef
import logging

from ganabosques_orm.collections.deforestation import Deforestation
from ganabosques_orm.collections.analysis import Analysis
from ganabosques_orm.collections.adm3 import Adm3
from ganabosques_orm.collections.adm3risk import Adm3Risk   # ✅ aquí están def_ha y farm_amount

# Para risk_total (flags por farm)
from ganabosques_orm.collections.farm import Farm
from ganabosques_orm.collections.farmrisk import FarmRisk

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
    type: Literal["annual", "cumulative", "warning", "quarter"]

class Adm3PeriodItem(BaseModel):
    period_start: Optional[str] = None  # ISODate
    period_end: Optional[str] = None    # ISODate
    risk_total: bool
    farm_amount: Optional[int] = None   # suma en el período (desde Adm3Risk)
    def_ha: Optional[float] = None      # suma en el período (desde Adm3Risk)

class Adm3Group(BaseModel):
    adm3_id: str
    name: Optional[str] = None
    department: Optional[str] = None
    municipality: Optional[str] = None
    items: List[Adm3PeriodItem] = Field(default_factory=list)

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

def _as_object_id(val):
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

def _split_label(label: Optional[str]):
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

    # 1) Validación
    try:
        valid_adm3_ids = _validate_object_ids(payload.adm3_ids, "adm3_ids")
    except HTTPException:
        raise
    except Exception as e:
        dump_exc("validación", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    defo_type = payload.type

    # 2) Metadatos ADM3 + init salida
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

    # 3) Farms por ADM3 (para calcular risk_total desde FarmRisk)
    try:
        farms = list(
            Farm.objects(adm3_id__in=valid_adm3_ids)
            .no_dereference()
            .only("id", "adm3_id")
        )
        farm_to_adm3: Dict[str, str] = {}
        if farms:
            for f in farms:
                adm3_id = _as_object_id(getattr(f, "adm3_id", None))
                if not adm3_id:
                    continue
                farm_to_adm3[str(f.id)] = str(adm3_id)
    except Exception as e:
        dump_exc("consulta Farms", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    # 4) Cargar Adm3Risk (fuente de farm_amount y def_ha)
    try:
        adm3risks = list(
            Adm3Risk.objects(adm3_id__in=valid_adm3_ids)
            .no_dereference()
            .only("id", "adm3_id", "analysis_id", "def_ha", "farm_amount")
        )
        if not adm3risks and not farms:
            dbg("No hay Adm3Risk ni Farms; retorno {}")
            return Adm3RiskGroupedResponse(root={})
    except Exception as e:
        dump_exc("consulta Adm3Risk", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    # 5) FarmRisk (para OR de flags) — opcional si hay farms
    try:
        farmrisks = []
        if farm_to_adm3:
            all_farm_ids = list({ObjectId(fid) if ObjectId.is_valid(fid) else fid for fid in farm_to_adm3.keys()})
            farmrisks = list(
                FarmRisk.objects(farm_id__in=all_farm_ids)
                .no_dereference()
                .only("id", "farm_id", "analysis_id", "risk_direct", "risk_input", "risk_output")
            )
    except Exception as e:
        dump_exc("consulta FarmRisk", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    # 6) Analyses (union de los usados por Adm3Risk y FarmRisk)
    try:
        analysis_ids_set = set()
        for r in adm3risks:
            aid = _as_object_id(getattr(r, "analysis_id", None))
            if aid:
                analysis_ids_set.add(aid)
        for fr in farmrisks:
            aid = _as_object_id(getattr(fr, "analysis_id", None))
            if aid:
                analysis_ids_set.add(aid)

        if not analysis_ids_set:
            dbg("No hay analyses vinculados; retorno {}")
            return Adm3RiskGroupedResponse(root={})

        analyses = list(
            Analysis.objects(id__in=list(analysis_ids_set))
            .no_dereference()
            .only("id", "deforestation_id")
        )
        if not analyses:
            dbg("No hay Analyses; retorno {}")
            return Adm3RiskGroupedResponse(root={})

        # Map analysis_id_str -> deforestation_id_str
        analysis_to_defo: Dict[str, str] = {}
        for a in analyses:
            did = _as_object_id(getattr(a, "deforestation_id", None))
            if did:
                analysis_to_defo[str(a.id)] = str(did)
    except Exception as e:
        dump_exc("consulta Analysis", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    # 7) Deforestations del type pedido
    try:
        defo_ids = list({ObjectId(v) for v in analysis_to_defo.values() if ObjectId.is_valid(v)})
        if not defo_ids:
            dbg("No hay deforestation_ids; retorno {}")
            return Adm3RiskGroupedResponse(root={})

        deforestations = list(
            Deforestation.objects(id__in=defo_ids, deforestation_type=defo_type)
            .no_dereference()
            .only("id", "period_start", "period_end", "deforestation_type")
        )
        if not deforestations:
            dbg("No hay Deforestations del type pedido; retorno {}")
            return Adm3RiskGroupedResponse(root={})

        # Map deforestation_id_str -> (period_start_iso, period_end_iso)
        defo_periods: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        for d in deforestations:
            son = d.to_mongo().to_dict()
            ps = son.get("period_start")
            pe = son.get("period_end")
            ps_iso = ps.isoformat() if ps else None
            pe_iso = pe.isoformat() if pe else None
            defo_periods[str(son["_id"])] = (ps_iso, pe_iso)

        defo_id_set = set(defo_periods.keys())
    except Exception as e:
        dump_exc("consulta Deforestation", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    # 8) Acumuladores por (ADM3, defo_id)
    try:
        # dict: adm3_id -> defo_id -> {risk_total, farm_amount, def_ha}
        acc: Dict[str, Dict[str, Dict[str, object]]] = {
            str(oid): {} for oid in valid_adm3_ids
        }

        # 8.a) Sumas desde Adm3Risk (farm_amount, def_ha)
        for r in adm3risks:
            rdoc = r.to_mongo().to_dict()
            adm3_id_str = str(_as_object_id(rdoc.get("adm3_id")) or rdoc.get("adm3_id"))
            analysis_id_str = str(_as_object_id(rdoc.get("analysis_id")) or rdoc.get("analysis_id"))
            defo_id_str = analysis_to_defo.get(analysis_id_str)
            if not defo_id_str or defo_id_str not in defo_id_set:
                continue

            bucket = acc.setdefault(adm3_id_str, {}).setdefault(defo_id_str, {
                "risk_total": False,
                "farm_amount": 0,
                "def_ha": 0.0
            })

            fm = rdoc.get("farm_amount")
            dh = rdoc.get("def_ha")
            if fm is not None:
                bucket["farm_amount"] = int(bucket["farm_amount"]) + int(fm)
            if dh is not None:
                bucket["def_ha"] = float(bucket["def_ha"]) + float(dh)

        # 8.b) OR de flags desde FarmRisk para risk_total
        for fr in farmrisks:
            fr_doc = fr.to_mongo().to_dict()
            farm_id_str = str(_as_object_id(fr_doc.get("farm_id")) or fr_doc.get("farm_id"))
            adm3_id_str = farm_to_adm3.get(farm_id_str)
            if not adm3_id_str:
                continue

            analysis_id_str = str(_as_object_id(fr_doc.get("analysis_id")) or fr_doc.get("analysis_id"))
            defo_id_str = analysis_to_defo.get(analysis_id_str)
            if not defo_id_str or defo_id_str not in defo_id_set:
                continue

            bucket = acc.setdefault(adm3_id_str, {}).setdefault(defo_id_str, {
                "risk_total": False,
                "farm_amount": 0,
                "def_ha": 0.0
            })

            any_flag = bool(fr_doc.get("risk_direct") or fr_doc.get("risk_input") or fr_doc.get("risk_output"))
            bucket["risk_total"] = bool(bucket["risk_total"] or any_flag)

        # 9) Construcción de respuesta
        for adm3_id_str, periods_map in acc.items():
            for defo_id_str, vals in periods_map.items():
                ps_iso, pe_iso = defo_periods.get(defo_id_str, (None, None))
                grouped.setdefault(adm3_id_str, Adm3Group(
                    adm3_id=adm3_id_str,
                    name=adm3_meta.get(adm3_id_str, {}).get("name"),
                    department=adm3_meta.get(adm3_id_str, {}).get("department"),
                    municipality=adm3_meta.get(adm3_id_str, {}).get("municipality"),
                    items=[]
                ))
                grouped[adm3_id_str].items.append(
                    Adm3PeriodItem(
                        period_start=ps_iso,
                        period_end=pe_iso,
                        risk_total=bool(vals["risk_total"]),
                        farm_amount=int(vals["farm_amount"]) if vals["farm_amount"] is not None else None,
                        def_ha=float(vals["def_ha"]) if vals["def_ha"] is not None else None,
                    )
                )

        return Adm3RiskGroupedResponse(root=grouped)

    except Exception as e:
        dump_exc("agrupación/serialización", e)
        raise HTTPException(status_code=500, detail="Internal server error")