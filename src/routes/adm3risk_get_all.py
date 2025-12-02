# routes/adm3risk_by_adm3_and_type.py
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field, RootModel
from typing import List, Dict, Literal, Optional, Tuple
from bson import ObjectId, DBRef

from ganabosques_orm.collections.adm3 import Adm3
from ganabosques_orm.collections.adm3risk import Adm3Risk
from ganabosques_orm.collections.analysis import Analysis
from ganabosques_orm.collections.deforestation import Deforestation

from dependencies.auth_guard import require_admin 

router = APIRouter(
    tags=["Adm3 Risk"],
    dependencies=[Depends(require_admin)]  
)

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

def _validate_object_ids(ids: List[str]) -> List[ObjectId]:
    out: List[ObjectId] = []
    for raw in ids:
        oid = _as_object_id(raw)
        if not oid:
            raise HTTPException(status_code=400, detail=f"Invalid ObjectId: {raw}")
        out.append(oid)
    return out

def _split_label(label: Optional[str]):
    if not label:
        return None, None, None
    parts = [p.strip() for p in str(label).split(",")]
    dep = parts[0] if len(parts) >= 1 else None
    mun = parts[1] if len(parts) >= 2 else None
    nm = parts[2] if len(parts) >= 3 else None
    return dep, mun, nm


@router.post("/adm3risk/by-adm3-and-type", response_model=Adm3RiskGroupedResponse)
def get_adm3risk_by_adm3_and_type(payload: RequestBody):
    try:
        valid_adm3_ids = _validate_object_ids(payload.adm3_ids)

        adm3_docs = list(
            Adm3.objects(id__in=valid_adm3_ids)
            .no_dereference()
            .only("id", "name", "label")
        )
        grouped: Dict[str, Adm3Group] = {}
        for d in adm3_docs:
            dep, mun, _ = _split_label(getattr(d, "label", None))
            grouped[str(d.id)] = Adm3Group(
                adm3_id=str(d.id),
                name=getattr(d, "name", None),
                department=dep,
                municipality=mun,
                items=[]
            )

        deforestations = list(
            Deforestation.objects(deforestation_type=payload.type)
            .no_dereference()
            .only("id", "period_start", "period_end")
        )
        defo_periods: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        for d in deforestations:
            doc = d.to_mongo().to_dict()
            ps, pe = doc.get("period_start"), doc.get("period_end")
            defo_periods[str(doc["_id"])] = (
                ps.isoformat() if ps else None,
                pe.isoformat() if pe else None,
            )
        if not defo_periods:
            return Adm3RiskGroupedResponse(root=grouped)

        analyses = list(
            Analysis.objects(deforestation_id__in=list(defo_periods.keys()))
            .no_dereference()
            .only("id", "deforestation_id")
        )
        analysis_to_defo: Dict[str, str] = {}
        for a in analyses:
            did = _as_object_id(getattr(a, "deforestation_id", None))
            if did:
                analysis_to_defo[str(a.id)] = str(did)

        coll = Adm3Risk._get_collection()
        cursor = list(
            coll.find(
                {
                    "analysis_id": {"$in": [ObjectId(aid) for aid in analysis_to_defo.keys()]},
                    "adm3_id": {"$in": valid_adm3_ids},
                },
                projection={
                    "_id": 0,
                    "analysis_id": 1,
                    "adm3_id": 1,
                    "risk_total": 1,
                    "farm_amount": 1,
                    "def_ha": 1,
                },
            )
        )
        existing_map = {
            (str(doc["adm3_id"]), str(doc["analysis_id"])): doc for doc in cursor
        }

        for adm3_id in [str(x) for x in valid_adm3_ids]:
            grouped.setdefault(adm3_id, Adm3Group(adm3_id=adm3_id, items=[]))

            for analysis_id, defo_id in analysis_to_defo.items():
                ps_iso, pe_iso = defo_periods.get(defo_id, (None, None))
                key = (adm3_id, analysis_id)
                doc = existing_map.get(key)

                grouped[adm3_id].items.append(
                    Adm3PeriodItem(
                        period_start=ps_iso,
                        period_end=pe_iso,
                        risk_total=bool(doc["risk_total"]) if doc else False,
                        farm_amount=int(doc["farm_amount"]) if doc else 0,
                        def_ha=float(doc["def_ha"]) if doc else 0.0,
                    )
                )

            grouped[adm3_id].items = list(reversed(grouped[adm3_id].items))

        return Adm3RiskGroupedResponse(root=grouped)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")