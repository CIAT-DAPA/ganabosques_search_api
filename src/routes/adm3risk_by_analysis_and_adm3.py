# routes/adm3risk_by_analysis_and_adm3.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Tuple
from bson import ObjectId
from bson.dbref import DBRef
import logging

from ganabosques_orm.collections.analysis import Analysis
from ganabosques_orm.collections.deforestation import Deforestation
from ganabosques_orm.collections.farm import Farm
from ganabosques_orm.collections.farmrisk import FarmRisk

router = APIRouter()

log = logging.getLogger("adm3risk_filtered")
logging.basicConfig(level=logging.INFO)

class Adm3RiskFilterRequest(BaseModel):
    analysis_ids: List[str]
    adm3_ids: List[str]

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

def _get_fr_ha(fr_doc) -> float:
    d = fr_doc.get("deforestation")
    if isinstance(d, dict):
        v = d.get("ha")
        if isinstance(v, (int, float)):
            return float(v)
    return 0.0

@router.post("/adm3risk/by-analysis-and-adm3")
def get_adm3risk_filtered(data: Adm3RiskFilterRequest):
    try:
        # 1) Validar ObjectIds
        if not data.analysis_ids or not data.adm3_ids:
            raise HTTPException(status_code=400, detail="analysis_ids y adm3_ids son requeridos")

        valid_analysis_ids: List[ObjectId] = []
        valid_adm3_ids: List[ObjectId] = []

        for a_id in data.analysis_ids:
            if ObjectId.is_valid(a_id):
                valid_analysis_ids.append(ObjectId(a_id))
            else:
                raise HTTPException(status_code=400, detail=f"Invalid analysis_id: {a_id}")

        for adm_id in data.adm3_ids:
            if ObjectId.is_valid(adm_id):
                valid_adm3_ids.append(ObjectId(adm_id))
            else:
                raise HTTPException(status_code=400, detail=f"Invalid adm3_id: {adm_id}")

        # 2) Mapear analysis -> deforestation -> periodos
        analyses = list(
            Analysis.objects(id__in=valid_analysis_ids)
            .no_dereference()
            .only("id", "deforestation_id")
        )
        if not analyses:
            return {str(aid): [] for aid in valid_analysis_ids}

        analysis_to_defo: Dict[str, str] = {}
        for a in analyses:
            did = _as_object_id(getattr(a, "deforestation_id", None))
            if did:
                analysis_to_defo[str(a.id)] = str(did)

        defo_ids = [ObjectId(d) for d in set(analysis_to_defo.values()) if ObjectId.is_valid(d)]
        deforestations = list(
            Deforestation.objects(id__in=defo_ids)
            .no_dereference()
            .only("id", "period_start", "period_end")
        )

        defo_periods: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        for d in deforestations:
            son = d.to_mongo().to_dict()
            ps = son.get("period_start")
            pe = son.get("period_end")
            ps_iso = ps.isoformat() if ps else None
            pe_iso = pe.isoformat() if pe else None
            defo_periods[str(son["_id"])] = (ps_iso, pe_iso)

        # 3) Farms de los ADM3 (para contar total de fincas y mapear FarmRisk->ADM3)
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
            return {str(aid): [] for aid in valid_analysis_ids}

        # 4) FarmRisk (flags y ha)
        all_farm_ids = [ObjectId(fid) for fid in farm_to_adm3.keys() if ObjectId.is_valid(fid)]
        farmrisks = list(
            FarmRisk.objects(
                farm_id__in=all_farm_ids,
                analysis_id__in=valid_analysis_ids
            )
            .no_dereference()
            .only("id", "farm_id", "analysis_id",
                  "risk_direct", "risk_input", "risk_output",
                  "deforestation")
        )

        # 5) Acumuladores por (analysis_id, adm3_id)
        acc: Dict[str, Dict[str, Dict[str, Any]]] = {}
        seen_farms_risk: Dict[Tuple[str, str, str], bool] = {}

        for fr in farmrisks:
            fr_doc = fr.to_mongo().to_dict()
            farm_id_str = str(_as_object_id(fr_doc.get("farm_id")) or fr_doc.get("farm_id"))
            adm3_id_str = farm_to_adm3.get(farm_id_str)
            if not adm3_id_str:
                continue

            analysis_id_str = str(_as_object_id(fr_doc.get("analysis_id")) or fr_doc.get("analysis_id"))
            if not analysis_id_str:
                continue

            bucket = acc.setdefault(analysis_id_str, {}).setdefault(adm3_id_str, {
                "risk_total": False,
                "def_ha": 0.0
            })

            any_flag = bool(fr_doc.get("risk_direct") or fr_doc.get("risk_input") or fr_doc.get("risk_output"))
            bucket["risk_total"] = bucket["risk_total"] or any_flag

            if any_flag:
                key = (analysis_id_str, adm3_id_str, farm_id_str)
                if key not in seen_farms_risk:
                    seen_farms_risk[key] = True
                    fr_ha = _get_fr_ha(fr_doc)
                    bucket["def_ha"] += fr_ha

        # 6) Construir salida agrupada por analysis_id
        grouped_results: Dict[str, List[Dict[str, Any]]] = {str(aid): [] for aid in valid_analysis_ids}

        for analysis in analyses:
            a_id_str = str(analysis.id)
            defo_id = _as_object_id(getattr(analysis, "deforestation_id", None))
            ps_iso, pe_iso = (None, None)
            if defo_id and str(defo_id) in defo_periods:
                ps_iso, pe_iso = defo_periods[str(defo_id)]

            for adm3_oid in valid_adm3_ids:
                adm3_id_str = str(adm3_oid)
                vals = acc.get(a_id_str, {}).get(adm3_id_str, {
                    "risk_total": False,
                    "def_ha": 0.0
                })
                grouped_results[a_id_str].append({
                    "analysis_id": a_id_str,
                    "adm3_id": adm3_id_str,
                    "period_start": ps_iso,
                    "period_end": pe_iso,
                    "risk_total": bool(vals["risk_total"]),
                    "farm_amount": adm3_farm_count.get(adm3_id_str, 0),
                    "def_ha": vals["def_ha"],
                })

        return grouped_results

    except HTTPException:
        raise
    except Exception as e:
        log.exception("Error en get_adm3risk_filtered")
        raise HTTPException(status_code=500, detail="Internal server error")