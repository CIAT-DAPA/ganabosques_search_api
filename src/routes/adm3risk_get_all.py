# routes/adm3risk_by_adm3_and_type.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, RootModel
from typing import List, Dict, Literal, Optional, Tuple
from bson import ObjectId
from bson.dbref import DBRef

from ganabosques_orm.collections.deforestation import Deforestation
from ganabosques_orm.collections.analysis import Analysis
from ganabosques_orm.collections.adm3 import Adm3
from ganabosques_orm.collections.farm import Farm
from ganabosques_orm.collections.farmrisk import FarmRisk

router = APIRouter()
MAX_IDS = 500

class RequestBody(BaseModel):
    adm3_ids: List[str] = Field(..., description="Lista de ObjectIds de ADM3")
    type: Literal["annual", "cumulative", "warning", "quarter"]

class Adm3PeriodItem(BaseModel):
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    risk_total: bool
    farm_amount: Optional[int] = None
    def_ha: Optional[float] = None

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

def _get_fr_ha(fr_doc) -> float:
    d = fr_doc.get("deforestation")
    if isinstance(d, dict):
        v = d.get("ha")
        if isinstance(v, (int, float)):
            return float(v)
    return 0.0

@router.post("/adm3risk/by-adm3-and-type", response_model=Adm3RiskGroupedResponse)
def get_adm3risk_by_adm3_and_type(payload: RequestBody):

    # 1) Validación
    valid_adm3_ids = _validate_object_ids(payload.adm3_ids, "adm3_ids")
    defo_type = payload.type

    # 2) Metadatos ADM3
    adm3_docs = list(
        Adm3.objects(id__in=valid_adm3_ids)
        .no_dereference()
        .only("id", "name", "label")
    )
    adm3_meta: Dict[str, Dict[str, Optional[str]]] = {}
    for d in adm3_docs:
        dep, mun, _ = _split_label(getattr(d, "label", None))
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

    # 3) Farms -> mapa FarmRisk->ADM3 + total de fincas
    farms = list(
        Farm.objects(adm3_id__in=valid_adm3_ids)
        .no_dereference()
        .only("id", "adm3_id")
    )
    farm_to_adm3: Dict[str, str] = {}
    adm3_farm_count: Dict[str, int] = {str(oid): 0 for oid in valid_adm3_ids}
    adm3_seen_farms: Dict[str, set] = {str(oid): set() for oid in valid_adm3_ids}

    for f in farms:
        adm3_id = _as_object_id(getattr(f, "adm3_id", None))
        if not adm3_id:
            continue
        adm3_id_str = str(adm3_id)
        farm_id_str = str(f.id)
        farm_to_adm3[farm_id_str] = adm3_id_str
        if farm_id_str not in adm3_seen_farms[adm3_id_str]:
            adm3_seen_farms[adm3_id_str].add(farm_id_str)
            adm3_farm_count[adm3_id_str] += 1

    if not farm_to_adm3:
        return Adm3RiskGroupedResponse(root={})

    # 4) FarmRisk
    all_farm_ids = [ObjectId(fid) for fid in farm_to_adm3.keys() if ObjectId.is_valid(fid)]
    farmrisks = list(
        FarmRisk.objects(farm_id__in=all_farm_ids)
        .no_dereference()
        .only("id", "farm_id", "analysis_id",
              "risk_direct", "risk_input", "risk_output",
              "deforestation")
    )
    if not farmrisks:
        return Adm3RiskGroupedResponse(root={})

    # 5) Analyses -> deforestation_id
    analysis_ids = {_as_object_id(fr.analysis_id) for fr in farmrisks if getattr(fr, "analysis_id", None)}
    analyses = list(
        Analysis.objects(id__in=list(analysis_ids))
        .no_dereference()
        .only("id", "deforestation_id")
    )
    analysis_to_defo: Dict[str, str] = {}
    for a in analyses:
        did = _as_object_id(getattr(a, "deforestation_id", None))
        if did:
            analysis_to_defo[str(a.id)] = str(did)

    # 6) Deforestations (para periodos y filtro por type)
    defo_ids = [ObjectId(v) for v in analysis_to_defo.values() if ObjectId.is_valid(v)]
    deforestations = list(
        Deforestation.objects(id__in=defo_ids, deforestation_type=defo_type)
        .no_dereference()
        .only("id", "period_start", "period_end")
    )
    defo_periods = {}
    for d in deforestations:
        son = d.to_mongo().to_dict()
        ps = son.get("period_start")
        pe = son.get("period_end")
        defo_periods[str(son["_id"])] = (
            ps.isoformat() if ps else None,
            pe.isoformat() if pe else None,
        )
    defo_id_set = set(defo_periods.keys())

    # 7) Acumuladores
    acc: Dict[str, Dict[str, Dict[str, object]]] = {str(oid): {} for oid in valid_adm3_ids}
    seen_farms_risk: Dict[Tuple[str, str], set] = {}

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

        any_flag = bool(fr_doc.get("risk_direct") or fr_doc.get("risk_input") or fr_doc.get("risk_output"))

        bucket = acc.setdefault(adm3_id_str, {}).setdefault(defo_id_str, {
            "risk_total": False,
            "def_ha": 0.0
        })

        bucket["risk_total"] = bucket["risk_total"] or any_flag

        if not any_flag:
            continue

        key = (adm3_id_str, defo_id_str)
        if key not in seen_farms_risk:
            seen_farms_risk[key] = set()

        if farm_id_str not in seen_farms_risk[key]:
            seen_farms_risk[key].add(farm_id_str)
            fr_ha = _get_fr_ha(fr_doc)
            bucket["def_ha"] += fr_ha

    # 8) Respuesta
    for adm3_id_str, periods_map in acc.items():
        for defo_id_str, vals in periods_map.items():
            ps_iso, pe_iso = defo_periods.get(defo_id_str, (None, None))
            grouped[adm3_id_str].items.append(
                Adm3PeriodItem(
                    period_start=ps_iso,
                    period_end=pe_iso,
                    risk_total=bool(vals["risk_total"]),
                    farm_amount=adm3_farm_count.get(adm3_id_str, 0),
                    def_ha=vals["def_ha"],
                )
            )

    return Adm3RiskGroupedResponse(root=grouped)