# routes/adm3risk_by_analysis_and_adm3.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Tuple
from bson import ObjectId
from bson.dbref import DBRef
import logging

from ganabosques_orm.collections.adm3risk import Adm3Risk
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

        # 3) Farms de los ADM3 (para computar risk_total desde FarmRisk)
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

        # 4) Adm3Risk (fuente de def_ha y farm_amount)
        adm3risks = list(
            Adm3Risk.objects(
                analysis_id__in=valid_analysis_ids,
                adm3_id__in=valid_adm3_ids
            )
            .no_dereference()
            .only("id", "adm3_id", "analysis_id", "def_ha", "farm_amount")
        )

        # 5) FarmRisk (fuente de flags booleanos para risk_total)
        farmrisks = []
        if farm_to_adm3:
            all_farm_ids = [ObjectId(fid) if ObjectId.is_valid(fid) else fid for fid in set(farm_to_adm3.keys())]
            farmrisks = list(
                FarmRisk.objects(
                    farm_id__in=all_farm_ids,
                    analysis_id__in=valid_analysis_ids
                )
                .no_dereference()
                .only("id", "farm_id", "analysis_id", "risk_direct", "risk_input", "risk_output")
            )

        # 6) Acumuladores por (analysis_id, adm3_id)
        # valores: {"risk_total": bool, "farm_amount": int, "def_ha": float}
        acc: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # Inicializar estructura de salida agrupada por analysis_id
        grouped_results: Dict[str, List[Dict[str, Any]]] = {str(aid): [] for aid in valid_analysis_ids}

        # 6.a) Sumas desde Adm3Risk (def_ha, farm_amount)
        for r in adm3risks:
            rdoc = r.to_mongo().to_dict()
            analysis_id_str = str(_as_object_id(rdoc.get("analysis_id")) or rdoc.get("analysis_id"))
            adm3_id_str = str(_as_object_id(rdoc.get("adm3_id")) or rdoc.get("adm3_id"))
            if not analysis_id_str or not adm3_id_str:
                continue

            acc.setdefault(analysis_id_str, {}).setdefault(adm3_id_str, {
                "risk_total": False,
                "farm_amount": 0,
                "def_ha": 0.0
            })

            fm = rdoc.get("farm_amount")
            dh = rdoc.get("def_ha")
            if fm is not None:
                acc[analysis_id_str][adm3_id_str]["farm_amount"] = int(acc[analysis_id_str][adm3_id_str]["farm_amount"]) + int(fm)
            if dh is not None:
                acc[analysis_id_str][adm3_id_str]["def_ha"] = float(acc[analysis_id_str][adm3_id_str]["def_ha"]) + float(dh)

        # 6.b) OR de flags desde FarmRisk (risk_total)
        for fr in farmrisks:
            fr_doc = fr.to_mongo().to_dict()
            farm_id_str = str(_as_object_id(fr_doc.get("farm_id")) or fr_doc.get("farm_id"))
            adm3_id_str = farm_to_adm3.get(farm_id_str)
            if not adm3_id_str:
                continue

            analysis_id_str = str(_as_object_id(fr_doc.get("analysis_id")) or fr_doc.get("analysis_id"))
            if not analysis_id_str:
                continue

            acc.setdefault(analysis_id_str, {}).setdefault(adm3_id_str, {
                "risk_total": False,
                "farm_amount": 0,
                "def_ha": 0.0
            })

            any_flag = bool(fr_doc.get("risk_direct") or fr_doc.get("risk_input") or fr_doc.get("risk_output"))
            acc[analysis_id_str][adm3_id_str]["risk_total"] = bool(acc[analysis_id_str][adm3_id_str]["risk_total"] or any_flag)

        # 7) Construir salida agrupada por analysis_id, enriqueciendo con periodos
        for analysis in analyses:
            a_id_str = str(analysis.id)
            defo_id = _as_object_id(getattr(analysis, "deforestation_id", None))
            ps_iso, pe_iso = (None, None)
            if defo_id and str(defo_id) in defo_periods:
                ps_iso, pe_iso = defo_periods[str(defo_id)]

            # Para cada adm3 solicitado, si no hay bucket lo devolvemos con ceros y risk_total=False
            for adm3_oid in valid_adm3_ids:
                adm3_id_str = str(adm3_oid)
                vals = acc.get(a_id_str, {}).get(adm3_id_str, {
                    "risk_total": False,
                    "farm_amount": 0,
                    "def_ha": 0.0
                })
                grouped_results[a_id_str].append({
                    "analysis_id": a_id_str,
                    "adm3_id": adm3_id_str,
                    "period_start": ps_iso,
                    "period_end": pe_iso,
                    "risk_total": bool(vals["risk_total"]),
                    "farm_amount": int(vals["farm_amount"]) if vals["farm_amount"] is not None else None,
                    "def_ha": float(vals["def_ha"]) if vals["def_ha"] is not None else None,
                })

        return grouped_results

    except HTTPException:
        raise
    except Exception as e:
        log.exception("Error en get_adm3risk_filtered")
        raise HTTPException(status_code=500, detail="Internal server error")