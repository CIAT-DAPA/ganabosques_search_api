# routes/adm3risk_by_analysis_and_adm3.py
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Tuple
from bson import ObjectId
from bson.dbref import DBRef
import logging

from ganabosques_orm.collections.analysis import Analysis
from ganabosques_orm.collections.deforestation import Deforestation
from ganabosques_orm.collections.adm3risk import Adm3Risk  

from dependencies.auth_guard import require_admin 

router = APIRouter(
    tags=["Adm3 Risk"],
    dependencies=[Depends(require_admin)]   
)

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
    s = str(val)
    return ObjectId(s) if ObjectId.is_valid(s) else None

def _safe_iso(dt) -> Optional[str]:
    try:
        return dt.isoformat() if dt else None
    except Exception:
        return None

@router.post("/adm3risk/by-analysis-and-adm3")
def get_adm3risk_filtered(data: Adm3RiskFilterRequest):
    try:
        if not data.analysis_ids or not data.adm3_ids:
            raise HTTPException(status_code=400, detail="analysis_ids y adm3_ids son requeridos")

        valid_analysis_ids = [ObjectId(a) for a in data.analysis_ids if ObjectId.is_valid(a)]
        valid_adm3_ids = [ObjectId(a) for a in data.adm3_ids if ObjectId.is_valid(a)]
        if not valid_analysis_ids or not valid_adm3_ids:
            raise HTTPException(status_code=400, detail="IDs inválidos")

        analyses = list(
            Analysis.objects(id__in=valid_analysis_ids)
            .no_dereference()
            .only("id", "deforestation_id")
        )
        defo_periods: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        defo_ids = []
        for a in analyses:
            did = _as_object_id(getattr(a, "deforestation_id", None))
            if did:
                defo_ids.append(did)
        if defo_ids:
            deforestations = list(
                Deforestation.objects(id__in=defo_ids)
                .no_dereference()
                .only("id", "period_start", "period_end")
            )
            for d in deforestations:
                mongo = d.to_mongo().to_dict()
                defo_periods[str(mongo["_id"])] = (
                    _safe_iso(mongo.get("period_start")),
                    _safe_iso(mongo.get("period_end")),
                )

        coll = Adm3Risk._get_collection()
        cursor = coll.find(
            {
                "analysis_id": {"$in": valid_analysis_ids},
                "adm3_id": {"$in": valid_adm3_ids},
            },
            projection={
                "_id": 0,
                "analysis_id": 1,
                "adm3_id": 1,
                "risk_total": 1,
                "def_ha": 1,
                "farm_amount": 1,
            },
        )

        by_pair: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for doc in cursor:
            a_id = str(doc["analysis_id"])
            adm_id = str(doc["adm3_id"])
            by_pair[(a_id, adm_id)] = {
                "risk_total": bool(doc.get("risk_total", False)),
                "def_ha": float(doc.get("def_ha", 0.0)) if doc.get("def_ha") is not None else 0.0,
                "farm_amount": int(doc.get("farm_amount", 0)) if doc.get("farm_amount") is not None else 0,
            }

        grouped_results: Dict[str, List[Dict[str, Any]]] = {str(a): [] for a in valid_analysis_ids}

        analysis_periods: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        for a in analyses:
            a_id_str = str(a.id)
            did = _as_object_id(getattr(a, "deforestation_id", None))
            ps_iso, pe_iso = (None, None)
            if did and str(did) in defo_periods:
                ps_iso, pe_iso = defo_periods[str(did)]
            analysis_periods[a_id_str] = (ps_iso, pe_iso)

        for analysis_id in valid_analysis_ids:
            a_id_str = str(analysis_id)
            ps_iso, pe_iso = analysis_periods.get(a_id_str, (None, None))

            for adm3_oid in valid_adm3_ids:
                adm3_id_str = str(adm3_oid)
                vals = by_pair.get((a_id_str, adm3_id_str))

                if vals:
                    grouped_results[a_id_str].append({
                        "analysis_id": a_id_str,
                        "adm3_id": adm3_id_str,
                        "period_start": ps_iso,
                        "period_end": pe_iso,
                        "risk_total": vals["risk_total"],
                        "farm_amount": vals["farm_amount"],
                        "def_ha": vals["def_ha"],
                    })
                else:
                    grouped_results[a_id_str].append({
                        "analysis_id": a_id_str,
                        "adm3_id": adm3_id_str,
                        "period_start": ps_iso,
                        "period_end": pe_iso,
                        "risk_total": False,
                        "farm_amount": 0,
                        "def_ha": 0.0,
                    })

        return grouped_results

    except HTTPException:
        raise
    except Exception as e:
        log.exception("Error en get_adm3risk_filtered")
        raise HTTPException(status_code=500, detail="Internal server error")