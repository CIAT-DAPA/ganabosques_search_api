# routes/enterprise_risk_details.py
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Iterable, Optional
from bson import ObjectId
from bson.dbref import DBRef
import datetime

from ganabosques_orm.collections.enterpriserisk import EnterpriseRisk
from ganabosques_orm.collections.farmrisk import FarmRisk
from ganabosques_orm.collections.farm import Farm
from ganabosques_orm.collections.enterprise import Enterprise
from ganabosques_orm.collections.analysis import Analysis
from ganabosques_orm.collections.deforestation import Deforestation
from ganabosques_orm.collections.farmpolygons import FarmPolygons

from ganabosques_orm.collections.adm1 import Adm1
from ganabosques_orm.collections.adm2 import Adm2

from dependencies.auth_guard import require_admin  

router = APIRouter(
    tags=["Enterprise Risk"],
    dependencies=[Depends(require_admin)] 
)

MAX_IDS = 500

class Request(BaseModel):
    analysis_id: str = Field(..., description="ObjectId del Analysis a consultar (vista actual)")
    enterprise_ids: List[str] = Field(default_factory=list, description="Enterprises a incluir; si vacío, se deduce de EnterpriseRisk")

def _as_object_id(val) -> Optional[ObjectId]:
    if val is None: return None
    if isinstance(val, ObjectId): return val
    if isinstance(val, DBRef): return val.id
    if isinstance(val, dict):
        if "$id" in val and ObjectId.is_valid(str(val["$id"])): return ObjectId(str(val["$id"]))
        if "$oid" in val and ObjectId.is_valid(str(val["$oid"])): return ObjectId(str(val["$oid"]))
    s = str(val)
    return ObjectId(s) if ObjectId.is_valid(s) else None

def _validate_oids(ids: Iterable[str], label: str) -> List[ObjectId]:
    ids = list(ids or [])
    if len(ids) > MAX_IDS:
        raise HTTPException(status_code=400, detail=f"'{label}' excede {MAX_IDS} IDs")
    out, seen = [], set()
    for raw in ids:
        if not ObjectId.is_valid(raw):
            raise HTTPException(status_code=400, detail=f"Invalid {label}: {raw}")
        if raw not in seen:
            seen.add(raw)
            out.append(ObjectId(raw))
    return out

def _stringify(v: Any) -> Any:
    if isinstance(v, ObjectId): return str(v)
    if isinstance(v, DBRef): return str(v.id)
    if isinstance(v, (datetime.datetime, datetime.date)): return v.isoformat()
    if isinstance(v, dict): return {k: _stringify(x) for k, x in v.items()}
    if isinstance(v, list): return [_stringify(x) for x in v]
    if isinstance(v, tuple): return tuple(_stringify(x) for x in v)
    return v

def _doc_to_dict(doc) -> Dict[str, Any]:
    return _stringify(doc.to_mongo().to_dict())


def _build_providers_from_er_list(
    ers: List[Dict[str, Any]],
    fr_by_id: Dict[str, Dict[str, Any]],
    farm_by_id: Dict[str, Dict[str, Any]],
    polygon_by_id: Dict[str, Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Devuelve providers con formato:
       {"inputs": [ farm{..., farm_polygon, risk}... ], "outputs": [ ... ]}"""
    inputs_entries: List[Dict[str, Any]] = []
    outputs_entries: List[Dict[str, Any]] = []

    for er in ers:
        # inputs
        for rid in (er.get("risk_input") or []):
            rid_str = str(_as_object_id(rid))
            fr = fr_by_id.get(rid_str)
            if not fr: 
                continue
            fid = str(_as_object_id(fr.get("farm_id")))
            farm = farm_by_id.get(fid)
            farm_payload = dict(farm) if isinstance(farm, dict) else {"_id": fid}
            fpid = str(_as_object_id(fr.get("farm_polygons_id")))
            farm_payload["farm_polygon"] = polygon_by_id.get(fpid)
            farm_payload["risk"] = fr
            inputs_entries.append(farm_payload)
        # outputs
        for rid in (er.get("risk_output") or []):
            rid_str = str(_as_object_id(rid))
            fr = fr_by_id.get(rid_str)
            if not fr:
                continue
            fid = str(_as_object_id(fr.get("farm_id")))
            farm = farm_by_id.get(fid)
            farm_payload = dict(farm) if isinstance(farm, dict) else {"_id": fid}
            fpid = str(_as_object_id(fr.get("farm_polygons_id")))
            farm_payload["farm_polygon"] = polygon_by_id.get(fpid)
            farm_payload["risk"] = fr
            outputs_entries.append(farm_payload)

    return {"inputs": inputs_entries, "outputs": outputs_entries}


@router.post("/enterprise-risk/details/by-enterprise")
def get_enterprise_risk_grouped_by_enterprise(payload: Request):
    analysis_oid = _as_object_id(payload.analysis_id)
    if not analysis_oid:
        raise HTTPException(status_code=400, detail="analysis_id inválido")
    enterprise_oids = _validate_oids(payload.enterprise_ids, "enterprise_ids")

    er_query_current = {"analysis_id": analysis_oid}
    if enterprise_oids:
        er_query_current["enterprise_id__in"] = enterprise_oids

    er_docs_current = list(
        EnterpriseRisk.objects(**er_query_current)
        .no_dereference()
        .only("id", "enterprise_id", "analysis_id", "risk_input", "risk_output")
    )
    er_list_current = [_doc_to_dict(er) for er in er_docs_current]

    if not enterprise_oids:
        enterprise_oids = [ObjectId(er["enterprise_id"]) for er in er_list_current if _as_object_id(er.get("enterprise_id"))]

    er_current_by_ent: Dict[str, List[Dict[str, Any]]] = {}
    for er in er_list_current:
        ent_id = str(_as_object_id(er.get("enterprise_id")))
        if ent_id:
            er_current_by_ent.setdefault(ent_id, []).append(er)

    farmrisk_ids_current: set[ObjectId] = set()
    for ers in er_current_by_ent.values():
        for er in ers:
            for rid in (er.get("risk_input") or []):
                oid = _as_object_id(rid)
                if oid: farmrisk_ids_current.add(oid)
            for rid in (er.get("risk_output") or []):
                oid = _as_object_id(rid)
                if oid: farmrisk_ids_current.add(oid)

    fr_dicts_current: List[Dict[str, Any]] = []
    if farmrisk_ids_current:
        fr_docs = list(
            FarmRisk.objects(id__in=list(farmrisk_ids_current))
            .no_dereference()
            .only(
                "id", "farm_id", "analysis_id", "farm_polygons_id",
                "deforestation", "protected", "farming_in", "farming_out",
                "risk_direct", "risk_input", "risk_output"
            )
        )
        fr_dicts_current = [_doc_to_dict(fr) for fr in fr_docs]
    fr_by_id_current: Dict[str, Dict[str, Any]] = {fr["_id"]: fr for fr in fr_dicts_current}

    farm_ids_current: set[ObjectId] = set()
    polygon_ids_current: set[ObjectId] = set()
    for fr in fr_dicts_current:
        fid = _as_object_id(fr.get("farm_id"))
        if fid: farm_ids_current.add(fid)
        fpid = _as_object_id(fr.get("farm_polygons_id"))
        if fpid: polygon_ids_current.add(fpid)

    farm_by_id_current: Dict[str, Dict[str, Any]] = {}
    if farm_ids_current:
        f_docs = list(
            Farm.objects(id__in=list(farm_ids_current))
            .no_dereference()
            .only("id", "adm3_id", "ext_id", "farm_source")
        )
        farm_by_id_current = {f["_id"]: f for f in (_doc_to_dict(x) for x in f_docs)}

    polygon_by_id_current: Dict[str, Dict[str, Any]] = {}
    if polygon_ids_current:
        fp_docs = list(
            FarmPolygons.objects(id__in=list(polygon_ids_current))
            .no_dereference()
            .only("id", "farm_id", "geojson", "latitude", "longitud", "farm_ha", "radio")
        )
        for fp in fp_docs:
            d = _doc_to_dict(fp)
            d.pop("_id", None)  
            polygon_by_id_current[str(_as_object_id(fp.id))] = d

    ent_ids_unique = list({str(eid) for eid in enterprise_oids})
    ent_oid_list = [ObjectId(eid) for eid in ent_ids_unique if ObjectId.is_valid(eid)]
    ent_docs = list(
        Enterprise.objects(id__in=ent_oid_list)
        .no_dereference()
        .only("id", "adm2_id", "name", "ext_id", "type_enterprise", "latitude", "longitud")
    )
    ent_list = [_doc_to_dict(e) for e in ent_docs]
    ent_by_id: Dict[str, Dict[str, Any]] = {e["_id"]: e for e in ent_list}

    adm2_oid_set: set[ObjectId] = set()
    for e in ent_list:
        a2 = _as_object_id(e.get("adm2_id"))
        if a2:
            adm2_oid_set.add(a2)

    adm2_by_id: Dict[str, Dict[str, Any]] = {}
    if adm2_oid_set:
        adm2_docs = list(
            Adm2.objects(id__in=list(adm2_oid_set))
            .no_dereference()
            .only("id", "name", "adm1_id")
        )
        adm2_by_id = {d["_id"]: d for d in (_doc_to_dict(x) for x in adm2_docs)}

    adm1_oid_set: set[ObjectId] = set()
    for a2 in adm2_by_id.values():
        a1 = _as_object_id(a2.get("adm1_id"))
        if a1:
            adm1_oid_set.add(a1)

    adm1_by_id: Dict[str, Dict[str, Any]] = {}
    if adm1_oid_set:
        adm1_docs = list(
            Adm1.objects(id__in=list(adm1_oid_set))
            .no_dereference()
            .only("id", "name")
        )
        adm1_by_id = {d["_id"]: d for d in (_doc_to_dict(x) for x in adm1_docs)}

    def build_current_providers(ent_id: str) -> Dict[str, List[Dict[str, Any]]]:
        ers = er_current_by_ent.get(ent_id, [])
        return _build_providers_from_er_list(ers, fr_by_id_current, farm_by_id_current, polygon_by_id_current)

    er_docs_hist = list(
        EnterpriseRisk.objects(enterprise_id__in=enterprise_oids)
        .no_dereference()
        .only("id", "enterprise_id", "analysis_id", "risk_input", "risk_output")
    )
    er_list_hist = [_doc_to_dict(er) for er in er_docs_hist]
    er_hist_by_ent: Dict[str, List[Dict[str, Any]]] = {}
    for er in er_list_hist:
        ent_id = str(_as_object_id(er.get("enterprise_id")))
        if ent_id:
            er_hist_by_ent.setdefault(ent_id, []).append(er)

    analysis_ids_hist: List[ObjectId] = []
    for er in er_list_hist:
        aid = _as_object_id(er.get("analysis_id"))
        if aid:
            analysis_ids_hist.append(aid)
    analysis_ids_hist = list({a for a in analysis_ids_hist})

    analyses_hist = list(
        Analysis.objects(id__in=analysis_ids_hist)
        .no_dereference()
        .only("id", "deforestation_id")
    )
    analysis_to_defo: Dict[str, str] = {}
    defo_ids_hist: List[ObjectId] = []
    for a in analyses_hist:
        ad = _doc_to_dict(a)
        did = _as_object_id(ad.get("deforestation_id"))
        if did:
            analysis_to_defo[ad["_id"]] = str(did)
            defo_ids_hist.append(did)
    defo_ids_hist = list({d for d in defo_ids_hist})

    defos_hist = list(
        Deforestation.objects(id__in=defo_ids_hist)
        .no_dereference()
        .only("id", "deforestation_type", "period_start", "period_end")
    )
    defo_by_id_hist: Dict[str, Dict[str, Any]] = {str(d.id): _doc_to_dict(d) for d in defos_hist}

    fr_ids_hist: set[ObjectId] = set()
    for er in er_list_hist:
        for rid in (er.get("risk_input") or []):
            oid = _as_object_id(rid)
            if oid: fr_ids_hist.add(oid)
        for rid in (er.get("risk_output") or []):
            oid = _as_object_id(rid)
            if oid: fr_ids_hist.add(oid)

    fr_by_id_hist: Dict[str, Dict[str, Any]] = {}
    farm_by_id_hist: Dict[str, Dict[str, Any]] = {}

    if fr_ids_hist:
        # FarmRisk
        fr_docs_h = list(
            FarmRisk.objects(id__in=list(fr_ids_hist))
            .no_dereference()
            .only(
                "id", "farm_id", "analysis_id", "farm_polygons_id",
                "deforestation", "protected", "farming_in", "farming_out",
                "risk_direct", "risk_input", "risk_output"
            )
        )
        fr_list_h = [_doc_to_dict(fr) for fr in fr_docs_h]
        fr_by_id_hist = {fr["_id"]: fr for fr in fr_list_h}

        # Farms
        farm_ids_h: set[ObjectId] = set()
        for fr in fr_list_h:
            fid = _as_object_id(fr.get("farm_id"))
            if fid: farm_ids_h.add(fid)

        if farm_ids_h:
            f_docs_h = list(
                Farm.objects(id__in=list(farm_ids_h))
                .no_dereference()
                .only("id", "adm3_id", "ext_id", "farm_source")
            )
            farm_by_id_hist = {f["_id"]: f for f in (_doc_to_dict(x) for x in f_docs_h)}

    def build_history_items_for_ent(ent_id: str) -> Dict[str, List[Dict[str, Any]]]:
        annual_items: List[Dict[str, Any]] = []
        cumulative_items: List[Dict[str, Any]] = []

        for er in er_hist_by_ent.get(ent_id, []):
            aid = str(_as_object_id(er.get("analysis_id")))
            did = analysis_to_defo.get(aid)
            if not did:
                continue
            defo = defo_by_id_hist.get(did)
            if not defo:
                continue
            dtype = str(defo.get("deforestation_type") or "").lower()
            if dtype not in ("annual", "cumulative"):
                continue

            providers = _build_providers_from_er_list(
                [er],
                fr_by_id_hist,
                farm_by_id_hist,
                {}  
            )
            item = {
                "period_start": defo.get("period_start"),
                "period_end":   defo.get("period_end"),
                "providers":    providers
            }
            if dtype == "annual":
                annual_items.append(item)
            else:
                cumulative_items.append(item)

        def _key(it): return (it.get("period_start") or "")
        annual_items.sort(key=_key)
        cumulative_items.sort(key=_key)

        return {"annual": annual_items, "cumulative": cumulative_items}

    enterprises_out: List[Dict[str, Any]] = []
    for ent_id, ent_doc in ent_by_id.items():
        current_providers = build_current_providers(ent_id)
        history_buckets   = build_history_items_for_ent(ent_id)

        adm2_id_str = str(_as_object_id(ent_doc.get("adm2_id")))
        adm2_doc    = adm2_by_id.get(adm2_id_str)
        adm1_doc    = None
        if adm2_doc:
            adm1_id_str = str(_as_object_id(adm2_doc.get("adm1_id")))
            adm1_doc = adm1_by_id.get(adm1_id_str)

        enterprises_out.append({
            "_id": ent_doc.get("_id"),
            "name": ent_doc.get("name"),
            "latitude": ent_doc.get("latitude"),
            "longitud": ent_doc.get("longitud"),
            "ext_id": ent_doc.get("ext_id"),
            "type_enterprise": ent_doc.get("type_enterprise"),

            "adm2_id": ent_doc.get("adm2_id"),

            "adm2": (
                {"_id": adm2_id_str, "name": adm2_doc.get("name")}
                if adm2_doc else None
            ),
            "adm1": (
                {
                    "_id": str(_as_object_id(adm2_doc.get("adm1_id"))),
                    "name": adm1_doc.get("name")
                }
                if (adm2_doc and adm1_doc) else None
            ),

            "providers": current_providers,  
            "history": history_buckets     
        })

    return enterprises_out