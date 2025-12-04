from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Dict, Any
from bson import ObjectId

from ganabosques_orm.collections.farmrisk import FarmRisk
from ganabosques_orm.collections.farm import Farm
from ganabosques_orm.collections.adm3 import Adm3

from dependencies.auth_guard import require_admin  

router = APIRouter(
    tags=["Farm Risk"],
    dependencies=[Depends(require_admin)]  
)

class FarmRiskFilterRequest(BaseModel):
    analysis_ids: List[str]
    farm_ids: List[str]

@router.post("/farmrisk/by-analysis-and-farm")
def get_farmrisk_filtered(data: FarmRiskFilterRequest):
    try:
        valid_analysis_ids: List[ObjectId] = []
        valid_farm_ids: List[ObjectId] = []

        # Validación de ObjectIds de entrada
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

        # Query de FarmRisk filtrado
        farmrisks_qs = FarmRisk.objects(
            analysis_id__in=valid_analysis_ids,
            farm_id__in=valid_farm_ids
        )

        # Lo convertimos a lista para poder iterar varias veces sin problemas
        farmrisks = list(farmrisks_qs)

        # ============================
        # 1) Resolver farms en bloque
        # ============================
        farm_ids_in_results = set()

        for fr in farmrisks:
            farm_ref = getattr(fr, "farm_id", None)
            if farm_ref is None:
                continue

            # Si es un objeto Farm (ReferenceField), usamos .id
            if hasattr(farm_ref, "id"):
                farm_ids_in_results.add(farm_ref.id)
            else:
                # Si ya es ObjectId o similar
                farm_ids_in_results.add(farm_ref)

        farms = Farm.objects(id__in=list(farm_ids_in_results))
        farms_by_id: Dict[ObjectId, Farm] = {f.id: f for f in farms}

        # ============================
        # 2) Resolver adm3 en bloque
        # ============================
        adm3_ids = set()

        for f in farms:
            adm3_ref = getattr(f, "adm3_id", None)
            if adm3_ref is None:
                continue

            if hasattr(adm3_ref, "id"):
                adm3_ids.add(adm3_ref.id)
            else:
                adm3_ids.add(adm3_ref)

        adm3_docs = Adm3.objects(id__in=list(adm3_ids))
        adm3_by_id: Dict[ObjectId, Adm3] = {a.id: a for a in adm3_docs}

        # Estructura agrupada por analysis_id (como ya tenías)
        grouped_results: Dict[str, List[Dict[str, Any]]] = {
            str(a_id): [] for a_id in valid_analysis_ids
        }

        # ============================
        # 3) Construir respuesta final
        # ============================
        for fr in farmrisks:
            doc = fr.to_mongo().to_dict()
            doc["_id"] = str(doc["_id"])

            # Normalizar IDs a string para el JSON
            farm_id_str = None
            farm_ref = doc.get("farm_id")

            if farm_ref is not None:
                # farm_ref aquí normalmente es ObjectId o DBRef en el dict
                farm_id_str = str(farm_ref)
                doc["farm_id"] = farm_id_str

            if "farm_polygons_id" in doc:
                doc["farm_polygons_id"] = str(doc["farm_polygons_id"])

            analysis_id_str = None
            if "analysis_id" in doc:
                analysis_id_val = doc["analysis_id"]
                analysis_id_str = str(analysis_id_val)
                doc["analysis_id"] = analysis_id_str

            # ---------------- Enriquecimiento con Adm3 ----------------
            department = None
            municipality = None
            vereda = None

            # Recuperar la Farm desde el objeto fr, que sí tiene la referencia viva
            farm_obj = getattr(fr, "farm_id", None)
            farm_id_for_lookup = None
            if farm_obj is not None:
                if hasattr(farm_obj, "id"):
                    farm_id_for_lookup = farm_obj.id
                else:
                    farm_id_for_lookup = farm_obj

            farm = farms_by_id.get(farm_id_for_lookup) if farm_id_for_lookup is not None else None

            adm3 = None
            if farm is not None:
                adm3_ref = getattr(farm, "adm3_id", None)
                if adm3_ref is not None:
                    if hasattr(adm3_ref, "id"):
                        adm3_id_for_lookup = adm3_ref.id
                    else:
                        adm3_id_for_lookup = adm3_ref
                    adm3 = adm3_by_id.get(adm3_id_for_lookup)

            if adm3 is not None:
                label = getattr(adm3, "label", None)
                if isinstance(label, str) and label.strip():
                    parts = [p.strip() for p in label.split(",")]
                    if len(parts) >= 1:
                        department = parts[0]
                    if len(parts) >= 2:
                        municipality = parts[1]
                    if len(parts) >= 3:
                        vereda = parts[2]

            # Añadir los 3 campos al documento final
            doc["department"] = department
            doc["municipality"] = municipality
            doc["vereda"] = vereda

            # Agregar al grupo correspondiente
            if analysis_id_str is not None and analysis_id_str in grouped_results:
                grouped_results[analysis_id_str].append(doc)
            else:
                grouped_results.setdefault(analysis_id_str or "unknown", []).append(doc)

        return grouped_results

    except HTTPException:
        # Re-lanzar las HTTPException explícitas (400, etc.)
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))