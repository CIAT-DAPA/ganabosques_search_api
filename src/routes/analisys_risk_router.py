from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Dict, Any
from bson import ObjectId
from bson.dbref import DBRef

from ganabosques_orm.collections.farmrisk import FarmRisk
from ganabosques_orm.collections.farm import Farm
from ganabosques_orm.collections.adm3 import Adm3
from ganabosques_orm.collections.farmriskverification import FarmRiskVerification
from dependencies.auth_guard import require_admin

router = APIRouter(tags=["Farm Risk"], dependencies=[Depends(require_admin)])

class FarmRiskFilterRequest(BaseModel):
    analysis_ids: List[str]
    farm_ids: List[str]


@router.post("/farmrisk/by-analysis-and-farm")
def get_farmrisk_filtered(data: FarmRiskFilterRequest):
    try:
        # ============================
        # 0) Validación de ObjectIds
        # ============================
        valid_analysis_ids: List[ObjectId] = []
        valid_farm_ids: List[ObjectId] = []

        for a_id in data.analysis_ids:
            if ObjectId.is_valid(a_id):
                valid_analysis_ids.append(ObjectId(a_id))
            else:
                raise HTTPException(status_code=400, detail=f"Invalid analysis_id: {a_id}")

        for f_id in data.farm_ids:
            if ObjectId.is_valid(f_id):
                valid_farm_ids.append(ObjectId(f_id))
            else:
                raise HTTPException(status_code=400, detail=f"Invalid farm_id: {f_id}")

        grouped_results: Dict[str, List[Dict[str, Any]]] = {str(a): [] for a in valid_analysis_ids}
        if not valid_analysis_ids or not valid_farm_ids:
            return grouped_results

        analysis_strs = [str(x) for x in valid_analysis_ids]
        farm_strs = [str(x) for x in valid_farm_ids]

        # ============================
        # 1) Query FarmRisk (1 query) + NO dereference + pocos campos
        # ============================
        raw_query = {
            "$and": [
                {"$or": [
                    {"analysis_id": {"$in": valid_analysis_ids}},
                    {"analysis_id.$id": {"$in": valid_analysis_ids}},
                    {"analysis_id": {"$in": analysis_strs}},
                ]},
                {"$or": [
                    {"farm_id": {"$in": valid_farm_ids}},
                    {"farm_id.$id": {"$in": valid_farm_ids}},
                    {"farm_id": {"$in": farm_strs}},
                ]},
            ]
        }

        # OJO: no_dereference evita que MongoEngine haga consultas extra al tocar fr.farm_id
        farmrisks_qs = (
            FarmRisk.objects(__raw__=raw_query)
            .no_dereference()
            # si sabes qué campos necesitas, limita. Ejemplo mínimo:
            # .only("analysis_id", "farm_id", "farm_polygons_id", "risk_input", "risk_output", "risk_direct")
        )

        farmrisks = list(farmrisks_qs)
        if not farmrisks:
            return grouped_results

        # ============================
        # 2) Sacar ids SIN dereference (desde to_mongo)
        # ============================
        def oid_from_maybe_dbref(x):
            if x is None:
                return None
            if hasattr(x, "id"):  # DBRef
                return x.id
            return x  # ObjectId o string

        # Convertimos una vez cada doc a dict crudo para evitar repetir to_mongo()
        raw_docs: List[Dict[str, Any]] = []
        farm_ids_in_results = set()

        for fr in farmrisks:
            d = fr.to_mongo().to_dict()
            raw_docs.append(d)

            fval = d.get("farm_id")
            fid = oid_from_maybe_dbref(fval)
            if isinstance(fid, ObjectId):
                farm_ids_in_results.add(fid)

        # ============================
        # 3) Resolver farms en bloque (solo campos necesarios)
        # ============================
        farms = list(
            Farm.objects(id__in=list(farm_ids_in_results))
            .no_dereference()
            .only("adm3_id")
        )
        farms_by_id = {f.id: f.to_mongo().to_dict() for f in farms}

        # ============================
        # 4) Resolver adm3 en bloque (solo label)
        # ============================
        adm3_ids = set()
        for fdoc in farms_by_id.values():
            adm3_val = fdoc.get("adm3_id")
            aid = oid_from_maybe_dbref(adm3_val)
            if isinstance(aid, ObjectId):
                adm3_ids.add(aid)

        adm3_docs = list(
            Adm3.objects(id__in=list(adm3_ids))
            .only("label")
        )
        adm3_by_id = {a.id: a.to_mongo().to_dict() for a in adm3_docs}

        # ============================
        # 5) Verificaciones (más reciente por farmrisk) en 1 query
        # ============================
        farmrisk_ids = [fr.id for fr in farmrisks]
        fr_dbrefs = [DBRef(FarmRisk._get_collection_name(), oid) for oid in farmrisk_ids]

        verifications = list(
            FarmRiskVerification.objects(__raw__={"farmrisk": {"$in": farmrisk_ids + fr_dbrefs}})
            .order_by("-verification")
            .only("farmrisk", "user_id", "verification", "observation", "status")
        )

        verification_by_farmrisk: Dict[ObjectId, Dict[str, Any]] = {}
        for v in verifications:
            vdoc = v.to_mongo().to_dict()
            fr_ref = vdoc.get("farmrisk")
            frid = oid_from_maybe_dbref(fr_ref)
            if frid is None:
                continue
            if frid not in verification_by_farmrisk:
                user_val = vdoc.get("user_id")
                uid = oid_from_maybe_dbref(user_val)
                verification_by_farmrisk[frid] = {
                    "user_id": str(uid) if uid is not None else None,
                    "verification_date": vdoc.get("verification").isoformat() if vdoc.get("verification") else None,
                    "observation": vdoc.get("observation"),
                    "status": vdoc.get("status") if vdoc.get("status") is not None else False,
                }

        # ============================
        # 6) Construir respuesta final (igual formato)
        # ============================
        for d in raw_docs:
            d["_id"] = str(d["_id"])

            # analysis_id string
            a_val = d.get("analysis_id")
            a_oid = oid_from_maybe_dbref(a_val)
            analysis_id_str = str(a_oid) if a_oid is not None else None
            d["analysis_id"] = analysis_id_str

            # farm_id string
            f_val = d.get("farm_id")
            f_oid = oid_from_maybe_dbref(f_val)
            d["farm_id"] = str(f_oid) if f_oid is not None else None

            if "farm_polygons_id" in d and d["farm_polygons_id"] is not None:
                d["farm_polygons_id"] = str(d["farm_polygons_id"])

            # Enriquecimiento adm3
            department = municipality = vereda = None

            if isinstance(f_oid, ObjectId) and f_oid in farms_by_id:
                farm_doc = farms_by_id[f_oid]
                adm3_val = farm_doc.get("adm3_id")
                adm3_oid = oid_from_maybe_dbref(adm3_val)
                if isinstance(adm3_oid, ObjectId) and adm3_oid in adm3_by_id:
                    label = adm3_by_id[adm3_oid].get("label")
                    if isinstance(label, str) and label.strip():
                        parts = [p.strip() for p in label.split(",")]
                        if len(parts) >= 1: department = parts[0]
                        if len(parts) >= 2: municipality = parts[1]
                        if len(parts) >= 3: vereda = parts[2]

            d["department"] = department
            d["municipality"] = municipality
            d["vereda"] = vereda

            # Verificación
            # (ojo: aquí la llave es farmrisk _id)
            frid = ObjectId(d["_id"]) if ObjectId.is_valid(d["_id"]) else None
            d["verification"] = verification_by_farmrisk.get(frid, {}) if frid else {}

            grouped_results.setdefault(analysis_id_str or "unknown", []).append(d)

        return grouped_results

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))