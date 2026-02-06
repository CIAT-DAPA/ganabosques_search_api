# routes/risk_global_by_ids_and_type.py

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Dict, Literal, Optional, Tuple, Any
from bson import ObjectId, DBRef

from dependencies.auth_guard import require_admin

# Collections
from ganabosques_orm.collections.adm3 import Adm3
from ganabosques_orm.collections.adm3risk import Adm3Risk

from ganabosques_orm.collections.farm import Farm
from ganabosques_orm.collections.farmrisk import FarmRisk

from ganabosques_orm.collections.enterprise import Enterprise
from ganabosques_orm.collections.enterpriserisk import EnterpriseRisk

from ganabosques_orm.collections.analysis import Analysis
from ganabosques_orm.collections.deforestation import Deforestation

from ganabosques_orm.collections.adm2 import Adm2
from ganabosques_orm.collections.adm1 import Adm1


router = APIRouter(
    tags=["Risk Global"],
    dependencies=[Depends(require_admin)]
)

MAX_IDS = 500
FARMRISK_IN_BATCH = 8000  # <- batch para farm_id $in (ajústalo si necesitas)

EntityType = Literal["adm3", "farm", "enterprise"]
DefType = Literal["annual", "cumulative", "atd", "nad"]


class GlobalRequest(BaseModel):
    entity_type: EntityType = Field(..., description="adm3 | farm | enterprise")
    ids: List[str] = Field(..., description="Lista de ObjectIds (depende de entity_type)")

    # Modo histórico (como antes)
    type: Optional[DefType] = Field(default=None, description="annual|cumulative|atd|nad")

    # Modos “rápidos”
    analysis_ids: Optional[List[str]] = Field(default=None, description="Lista de analysis_id (si viene, ignora type)")
    deforestation_ids: Optional[List[str]] = Field(default=None, description="Lista de deforestation_id (si viene, ignora type)")


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


def _validate_object_ids(ids: List[str]) -> List[ObjectId]:
    if len(ids) > MAX_IDS:
        raise HTTPException(status_code=400, detail=f"Too many ids (max {MAX_IDS})")
    out: List[ObjectId] = []
    for raw in ids:
        oid = _as_object_id(raw)
        if not oid:
            raise HTTPException(status_code=400, detail=f"Invalid ObjectId: {raw}")
        out.append(oid)
    return out


def _iso(dt):
    try:
        return dt.isoformat() if dt else None
    except Exception:
        return None


def _split_label_3(label: Optional[str]):
    # "DEP, MUN, VEREDA"
    if not label:
        return None, None, None
    parts = [p.strip() for p in str(label).split(",")]
    dep = parts[0] if len(parts) >= 1 else None
    mun = parts[1] if len(parts) >= 2 else None
    ver = parts[2] if len(parts) >= 3 else None
    return dep, mun, ver


def _area(obj: Any) -> Dict[str, float]:
    if not isinstance(obj, dict):
        return {"ha": 0.0, "prop": 0.0}
    return {"ha": float(obj.get("ha") or 0.0), "prop": float(obj.get("prop") or 0.0)}


def _to_oid_list(val: Any) -> List[ObjectId]:
    if not val:
        return []
    if isinstance(val, list):
        out = []
        for x in val:
            ox = _as_object_id(x)
            if ox:
                out.append(ox)
        return out
    ox = _as_object_id(val)
    return [ox] if ox else []


def _extract_sit_codes_from_farm_ext_id(ext_id: Any) -> List[str]:
    codes: List[str] = []
    if not isinstance(ext_id, list):
        return codes
    for obj in ext_id:
        if not isinstance(obj, dict):
            continue
        if obj.get("source") == "SIT_CODE":
            code = obj.get("ext_code")
            if code is not None:
                codes.append(str(code))
    return codes


def _uniq(seq: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in seq:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _chunks(lst: List[Any], size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


# ----------------------------
# Shared: periods + analyses (3 modos)
# ----------------------------

def _get_periods_and_analyses(payload: GlobalRequest) -> Tuple[Dict[str, Tuple[Optional[str], Optional[str]]], Dict[str, str]]:
    """
    Devuelve:
      - defo_periods: deforestation_id(str) -> (period_start_iso, period_end_iso)
      - analysis_to_defo: analysis_id(str) -> deforestation_id(str)

    Modos:
      A) analysis_ids (más rápido)
      B) deforestation_ids
      C) type (histórico)
    """
    # A) analysis_ids
    if payload.analysis_ids:
        analysis_oids = _validate_object_ids(payload.analysis_ids)

        analyses = list(
            Analysis.objects(id__in=analysis_oids)
            .no_dereference()
            .only("id", "deforestation_id")
        )

        analysis_to_defo: Dict[str, str] = {}
        defo_oids: List[ObjectId] = []
        for a in analyses:
            did = _as_object_id(getattr(a, "deforestation_id", None))
            if did:
                analysis_to_defo[str(a.id)] = str(did)
                defo_oids.append(did)

        defo_oids = list({x for x in defo_oids})
        if not defo_oids or not analysis_to_defo:
            return {}, {}

        defos = list(
            Deforestation.objects(id__in=defo_oids)
            .no_dereference()
            .only("id", "period_start", "period_end")
        )
        defo_periods: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        for d in defos:
            doc = d.to_mongo().to_dict()
            ps, pe = doc.get("period_start"), doc.get("period_end")
            defo_periods[str(doc["_id"])] = (ps.isoformat() if ps else None, pe.isoformat() if pe else None)

        return defo_periods, analysis_to_defo

    # B) deforestation_ids
    if payload.deforestation_ids:
        defo_oids = _validate_object_ids(payload.deforestation_ids)

        defos = list(
            Deforestation.objects(id__in=defo_oids)
            .no_dereference()
            .only("id", "period_start", "period_end")
        )
        defo_periods: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        for d in defos:
            doc = d.to_mongo().to_dict()
            ps, pe = doc.get("period_start"), doc.get("period_end")
            defo_periods[str(doc["_id"])] = (ps.isoformat() if ps else None, pe.isoformat() if pe else None)

        if not defo_periods:
            return {}, {}

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

        return defo_periods, analysis_to_defo

    # C) type
    if not payload.type:
        raise HTTPException(status_code=400, detail="Must provide either type OR analysis_ids OR deforestation_ids")

    deforestations = list(
        Deforestation.objects(deforestation_type=payload.type)
        .no_dereference()
        .only("id", "period_start", "period_end")
    )
    defo_periods: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    for d in deforestations:
        doc = d.to_mongo().to_dict()
        ps, pe = doc.get("period_start"), doc.get("period_end")
        defo_periods[str(doc["_id"])] = (ps.isoformat() if ps else None, pe.isoformat() if pe else None)

    if not defo_periods:
        return {}, {}

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

    return defo_periods, analysis_to_defo


# ----------------------------
# FAST helper for ADM3 sit_codes
# ----------------------------

def _build_adm3_sit_codes_for_analysis(
    analysis_oid: ObjectId,
    farm_ids_for_adm3s: List[ObjectId],
    farm_to_adm3: Dict[str, str],
    farm_to_sit: Dict[str, List[str]],
) -> Dict[str, Dict[str, List[str]]]:
    """
    Retorna:
      {
        "<adm3_id>": {
           "direct": [...],
           "input": [...],
           "output": [...]
        },
        ...
      }
    """

    # acumuladores por adm3
    acc_direct: Dict[str, List[str]] = {}
    acc_input: Dict[str, List[str]] = {}
    acc_output: Dict[str, List[str]] = {}

    coll_fr = FarmRisk._get_collection()

    # IMPORTANT: batch farm_id $in
    for batch in _chunks(farm_ids_for_adm3s, FARMRISK_IN_BATCH):
        fr_rows = list(
            coll_fr.find(
                {"analysis_id": analysis_oid, "farm_id": {"$in": batch}},
                projection={"_id": 0, "farm_id": 1, "risk_direct": 1, "risk_input": 1, "risk_output": 1},
            )
        )

        for r in fr_rows:
            fid = _as_object_id(r.get("farm_id"))
            if not fid:
                continue
            fid_s = str(fid)

            adm3_id = farm_to_adm3.get(fid_s)
            if not adm3_id:
                continue

            sit_codes = farm_to_sit.get(fid_s, [])
            if not sit_codes:
                continue

            if r.get("risk_direct"):
                acc_direct.setdefault(adm3_id, []).extend(sit_codes)
            if r.get("risk_input"):
                acc_input.setdefault(adm3_id, []).extend(sit_codes)
            if r.get("risk_output"):
                acc_output.setdefault(adm3_id, []).extend(sit_codes)

    # unique final
    out: Dict[str, Dict[str, List[str]]] = {}
    all_adm3 = set(list(acc_direct.keys()) + list(acc_input.keys()) + list(acc_output.keys()))
    for adm3_id in all_adm3:
        out[adm3_id] = {
            "direct": _uniq(acc_direct.get(adm3_id, [])),
            "input": _uniq(acc_input.get(adm3_id, [])),
            "output": _uniq(acc_output.get(adm3_id, [])),
        }
    return out


# ----------------------------
# One endpoint
# ----------------------------

@router.post("/risk/by-ids-and-type")
def get_risk_by_ids_and_type(payload: GlobalRequest):
    print(">>> PAYLOAD QUE LLEGÓ AL ENDPOINT:", payload.dict())
    """
    Un solo endpoint:
      - entity_type=adm3: igual que /adm3risk/by-adm3-and-type
        + sit_codes {direct,input,output} (rápido)
      - entity_type=farm: farmrisk + farm meta
      - entity_type=enterprise: enterprise + adm1/adm2 names + SIT_CODEs
    Soporta modos rápidos: analysis_ids o deforestation_ids.
    """
    try:
        valid_ids = _validate_object_ids(payload.ids)
        defo_periods, analysis_to_defo = _get_periods_and_analyses(payload)

        # ------------------------------ ADM3 ------------------------------
        if payload.entity_type == "adm3":
            grouped: Dict[str, Any] = {}

            # base adm3 info
            adm3_docs = list(
                Adm3.objects(id__in=valid_ids).no_dereference().only("id", "name", "label")
            )
            for d in adm3_docs:
                dep, mun, _ = _split_label_3(getattr(d, "label", None))
                grouped[str(d.id)] = {
                    "adm3_id": str(d.id),
                    "name": getattr(d, "name", None),
                    "department": dep,
                    "municipality": mun,
                    "items": []
                }

            if not defo_periods or not analysis_to_defo:
                return grouped

            # 1) adm3risk docs (rápido, precomputado)
            coll_ar = Adm3Risk._get_collection()
            cursor = list(
                coll_ar.find(
                    {
                        "analysis_id": {"$in": [ObjectId(aid) for aid in analysis_to_defo.keys()]},
                        "adm3_id": {"$in": valid_ids},
                    },
                    projection={"_id": 0, "analysis_id": 1, "adm3_id": 1, "risk_total": 1, "farm_amount": 1, "def_ha": 1},
                )
            )
            existing_map = {(str(doc["adm3_id"]), str(doc["analysis_id"])): doc for doc in cursor}

            # 2) Farms SOLO de esos adm3 (para SIT_CODEs)
            farm_docs = list(
                Farm.objects(adm3_id__in=valid_ids)
                .no_dereference()
                .only("id", "adm3_id", "ext_id")
            )

            farm_ids_for_adm3s: List[ObjectId] = []
            farm_to_adm3: Dict[str, str] = {}
            farm_to_sit: Dict[str, List[str]] = {}

            for f in farm_docs:
                fm = f.to_mongo().to_dict()
                fid = _as_object_id(fm.get("_id"))
                adm3_oid = _as_object_id(fm.get("adm3_id"))
                if not fid or not adm3_oid:
                    continue

                fid_s = str(fid)
                farm_ids_for_adm3s.append(fid)
                farm_to_adm3[fid_s] = str(adm3_oid)
                farm_to_sit[fid_s] = _extract_sit_codes_from_farm_ext_id(fm.get("ext_id"))

            # 3) Por cada analysis, calcular sit_codes filtrando FarmRisk por farm_id IN esas farms
            #    (esto evita el query gigante FarmRisk.find({analysis_id}))
            analysis_sit_cache: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
            for analysis_id in analysis_to_defo.keys():
                analysis_oid = ObjectId(analysis_id)
                analysis_sit_cache[analysis_id] = _build_adm3_sit_codes_for_analysis(
                    analysis_oid=analysis_oid,
                    farm_ids_for_adm3s=farm_ids_for_adm3s,
                    farm_to_adm3=farm_to_adm3,
                    farm_to_sit=farm_to_sit,
                )

            # 4) armar items
            for adm3_oid in valid_ids:
                adm3_id = str(adm3_oid)
                grouped.setdefault(adm3_id, {"adm3_id": adm3_id, "items": []})

                for analysis_id, defo_id in analysis_to_defo.items():
                    ps_iso, pe_iso = defo_periods.get(defo_id, (None, None))
                    doc = existing_map.get((adm3_id, analysis_id))

                    sit_codes_for_adm3 = analysis_sit_cache.get(analysis_id, {}).get(adm3_id, {
                        "direct": [],
                        "input": [],
                        "output": [],
                    })

                    grouped[adm3_id]["items"].append({
                        "period_start": ps_iso,
                        "period_end": pe_iso,
                        "analysis_id": analysis_id,
                        "risk_total": bool(doc["risk_total"]) if doc else False,
                        "farm_amount": int(doc["farm_amount"]) if doc else 0,
                        "def_ha": float(doc["def_ha"]) if doc else 0.0,
                        "sit_codes": sit_codes_for_adm3,
                    })

                grouped[adm3_id]["items"] = list(reversed(grouped[adm3_id]["items"]))

            return grouped

        # ------------------------------ FARM ------------------------------
        if payload.entity_type == "farm":
            farm_docs = list(
                Farm.objects(id__in=valid_ids).no_dereference().only("id", "adm3_id", "ext_id", "log")
            )

            adm3_ids: List[ObjectId] = []
            farm_meta_map: Dict[str, Any] = {}
            for f in farm_docs:
                fm = f.to_mongo().to_dict()
                fid = _as_object_id(fm.get("_id"))
                if not fid:
                    continue
                adm3_oid = _as_object_id(fm.get("adm3_id"))
                if adm3_oid:
                    adm3_ids.append(adm3_oid)

                farm_meta_map[str(fid)] = {
                    "farm_id": str(fid),
                    "adm3_id": str(adm3_oid) if adm3_oid else None,
                    "ext_id": fm.get("ext_id") or None,
                    "log": fm.get("log") or None,
                    "department": None,
                    "municipality": None,
                    "vereda": None,
                }

            adm3_ids = list({x for x in adm3_ids})
            adm3_docs = list(Adm3.objects(id__in=adm3_ids).no_dereference().only("id", "label"))
            label_map = {str(a.id): _split_label_3(getattr(a, "label", None)) for a in adm3_docs}

            for meta in farm_meta_map.values():
                if meta["adm3_id"] and meta["adm3_id"] in label_map:
                    dep, mun, ver = label_map[meta["adm3_id"]]
                    meta["department"], meta["municipality"], meta["vereda"] = dep, mun, ver

            grouped: Dict[str, Any] = {
                str(fid): {"farm_id": str(fid), "farm": farm_meta_map.get(str(fid)), "items": []}
                for fid in valid_ids
            }

            if not defo_periods or not analysis_to_defo:
                return grouped

            coll = FarmRisk._get_collection()
            cursor = list(
                coll.find(
                    {
                        "analysis_id": {"$in": [ObjectId(aid) for aid in analysis_to_defo.keys()]},
                        "farm_id": {"$in": valid_ids},
                    },
                    projection={
                        "_id": 0, "analysis_id": 1, "farm_id": 1,
                        "risk_direct": 1, "risk_input": 1, "risk_output": 1,
                        "deforestation": 1, "farming_in": 1, "farming_out": 1, "protected": 1,
                    },
                )
            )
            existing_map = {(str(doc["farm_id"]), str(doc["analysis_id"])): doc for doc in cursor}

            for farm_oid in valid_ids:
                farm_id = str(farm_oid)
                for analysis_id, defo_id in analysis_to_defo.items():
                    ps_iso, pe_iso = defo_periods.get(defo_id, (None, None))
                    doc = existing_map.get((farm_id, analysis_id))

                    grouped[farm_id]["items"].append({
                        "period_start": ps_iso,
                        "period_end": pe_iso,
                        "analysis_id": analysis_id,
                        "risk_direct": bool(doc.get("risk_direct")) if doc else False,
                        "risk_input": bool(doc.get("risk_input")) if doc else False,
                        "risk_output": bool(doc.get("risk_output")) if doc else False,
                        "deforestation": _area(doc.get("deforestation")) if doc else _area(None),
                        "farming_in": _area(doc.get("farming_in")) if doc else _area(None),
                        "farming_out": _area(doc.get("farming_out")) if doc else _area(None),
                        "protected": _area(doc.get("protected")) if doc else _area(None),
                    })

                grouped[farm_id]["items"] = list(reversed(grouped[farm_id]["items"]))

            return grouped

        # ------------------------------ ENTERPRISE ------------------------------
        if payload.entity_type == "enterprise":
            coll_ent = Enterprise._get_collection()
            enterprise_docs = list(
                coll_ent.find(
                    {"_id": {"$in": valid_ids}},
                    projection={
                        "_id": 1,
                        "adm2_id": 1,
                        "name": 1,
                        "ext_id": 1,
                        "type_enterprise": 1,
                        "latitude": 1,
                        "longitud": 1,  # si tu campo es "longitude", cámbialo aquí
                        "log": 1,
                    },
                )
            )

            adm2_ids: List[ObjectId] = []
            enterprise_meta_map: Dict[str, Any] = {}
            for em in enterprise_docs:
                eid = _as_object_id(em.get("_id"))
                if not eid:
                    continue

                adm2_oid = _as_object_id(em.get("adm2_id"))
                if adm2_oid:
                    adm2_ids.append(adm2_oid)

                log = em.get("log") or {}

                enterprise_meta_map[str(eid)] = {
                    "enterprise_id": str(eid),
                    "type_enterprise": em.get("type_enterprise"),
                    "adm2_id": str(adm2_oid) if adm2_oid else None,
                    "name": em.get("name"),
                    "ext_id": em.get("ext_id") or None,
                    "latitude": float(em.get("latitude")) if em.get("latitude") is not None else None,
                    "longitud": float(em.get("longitud")) if em.get("longitud") is not None else None,
                    "log": log or None,
                    "created": _iso(log.get("created")),
                    "updated": _iso(log.get("updated")),
                    "municipality": None,
                    "department": None,
                }

            # adm2 -> adm1 names
            adm2_ids = list({x for x in adm2_ids})
            adm2_docs = list(Adm2.objects(id__in=adm2_ids).no_dereference().only("id", "name", "adm1_id"))

            adm2_name_map: Dict[str, str] = {}
            adm2_to_adm1: Dict[str, ObjectId] = {}
            adm1_ids: List[ObjectId] = []
            for a2 in adm2_docs:
                a2m = a2.to_mongo().to_dict()
                a2id = _as_object_id(a2m.get("_id"))
                if not a2id:
                    continue
                adm2_name_map[str(a2id)] = a2m.get("name") or ""
                a1id = _as_object_id(a2m.get("adm1_id"))
                if a1id:
                    adm2_to_adm1[str(a2id)] = a1id
                    adm1_ids.append(a1id)

            adm1_ids = list({x for x in adm1_ids})
            adm1_docs = list(Adm1.objects(id__in=adm1_ids).no_dereference().only("id", "name"))
            adm1_name_map = {str(a1.id): (getattr(a1, "name", None) or "") for a1 in adm1_docs}

            for meta in enterprise_meta_map.values():
                if meta["adm2_id"]:
                    meta["municipality"] = adm2_name_map.get(meta["adm2_id"]) or None
                    a1oid = adm2_to_adm1.get(meta["adm2_id"])
                    if a1oid:
                        meta["department"] = adm1_name_map.get(str(a1oid)) or None

            grouped: Dict[str, Any] = {
                str(eid): {"enterprise_id": str(eid), "enterprise": enterprise_meta_map.get(str(eid)), "items": []}
                for eid in valid_ids
            }

            if not defo_periods or not analysis_to_defo:
                return grouped

            coll_er = EnterpriseRisk._get_collection()
            er_docs = list(
                coll_er.find(
                    {
                        "analysis_id": {"$in": [ObjectId(aid) for aid in analysis_to_defo.keys()]},
                        "enterprise_id": {"$in": valid_ids},
                    },
                    projection={"_id": 0, "analysis_id": 1, "enterprise_id": 1, "risk_input": 1, "risk_output": 1},
                )
            )
            er_map = {(str(d["enterprise_id"]), str(d["analysis_id"])): d for d in er_docs}

            # farmrisk ids referenciados
            all_fr_oids: List[ObjectId] = []
            for d in er_docs:
                all_fr_oids += _to_oid_list(d.get("risk_input"))
                all_fr_oids += _to_oid_list(d.get("risk_output"))
            all_fr_oids = list({x for x in all_fr_oids})

            # farmrisk -> farm_id
            fr_to_farm: Dict[str, ObjectId] = {}
            farm_sit_map: Dict[str, List[str]] = {}

            if all_fr_oids:
                coll_fr = FarmRisk._get_collection()
                fr_rows = list(coll_fr.find({"_id": {"$in": all_fr_oids}}, projection={"_id": 1, "farm_id": 1}))
                farm_ids: List[ObjectId] = []
                for r in fr_rows:
                    frid = _as_object_id(r.get("_id"))
                    fid = _as_object_id(r.get("farm_id"))
                    if frid and fid:
                        fr_to_farm[str(frid)] = fid
                        farm_ids.append(fid)

                farm_ids = list({x for x in farm_ids})
                farm_docs2 = list(Farm.objects(id__in=farm_ids).no_dereference().only("id", "ext_id"))
                for f in farm_docs2:
                    fm = f.to_mongo().to_dict()
                    fid = str(fm.get("_id"))
                    farm_sit_map[fid] = _extract_sit_codes_from_farm_ext_id(fm.get("ext_id"))

            for enterprise_oid in valid_ids:
                enterprise_id = str(enterprise_oid)
                for analysis_id, defo_id in analysis_to_defo.items():
                    ps_iso, pe_iso = defo_periods.get(defo_id, (None, None))
                    doc = er_map.get((enterprise_id, analysis_id))

                    risk_in_raw = doc.get("risk_input") if doc else None
                    risk_out_raw = doc.get("risk_output") if doc else None

                    in_codes: List[str] = []
                    for frid in _to_oid_list(risk_in_raw):
                        farm_oid = fr_to_farm.get(str(frid))
                        if farm_oid:
                            in_codes += farm_sit_map.get(str(farm_oid), [])

                    out_codes: List[str] = []
                    for frid in _to_oid_list(risk_out_raw):
                        farm_oid = fr_to_farm.get(str(frid))
                        if farm_oid:
                            out_codes += farm_sit_map.get(str(farm_oid), [])

                    grouped[enterprise_id]["items"].append({
                        "period_start": ps_iso,
                        "period_end": pe_iso,
                        "analysis_id": analysis_id,
                        "risk_input": [str(_as_object_id(x) or x) for x in (risk_in_raw or [])] if isinstance(risk_in_raw, list) else (
                            [str(_as_object_id(risk_in_raw) or risk_in_raw)] if risk_in_raw else None
                        ),
                        "risk_output": [str(_as_object_id(x) or x) for x in (risk_out_raw or [])] if isinstance(risk_out_raw, list) else (
                            [str(_as_object_id(risk_out_raw) or risk_out_raw)] if risk_out_raw else None
                        ),
                        "sit_codes": {"input": _uniq(in_codes), "output": _uniq(out_codes)},
                    })

                grouped[enterprise_id]["items"] = list(reversed(grouped[enterprise_id]["items"]))

            return grouped

        raise HTTPException(status_code=400, detail=f"Invalid entity_type: {payload.entity_type}")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")