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
        valid_analysis_ids = []
        valid_farm_ids = []

        # Validación de ObjectIds
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

        farmrisks = FarmRisk.objects(
            analysis_id__in=valid_analysis_ids,
            farm_id__in=valid_farm_ids
        )

        # 1) Obtener todos los farm_id usados en los farmrisks
        farm_ids_in_results = {
            fr.farm_id for fr in farmrisks 
            if getattr(fr, "farm_id", None) is not None
        }

        # 2) Cargar todas las farms de una sola vez
        farms = Farm.objects(id__in=list(farm_ids_in_results))
        farms_by_id = {f.id: f for f in farms}

        # 3) Obtener todos los adm3_id a partir de las farms
        adm3_ids = {
            getattr(f, "adm3_id", None)
            for f in farms
            if getattr(f, "adm3_id", None) is not None
        }

        # 4) Cargar todos los Adm3 de una sola vez
        adm3_docs = Adm3.objects(id__in=list(adm3_ids))
        adm3_by_id = {a.id: a for a in adm3_docs}

        # Estructura agrupada por analysis_id (como ya tenías)
        grouped_results: Dict[str, List[Dict[str, Any]]] = {
            str(a_id): [] for a_id in valid_analysis_ids
        }

        for fr in farmrisks:
            doc = fr.to_mongo().to_dict()
            doc["_id"] = str(doc["_id"])

            # Normalizar IDs a string
            if "farm_id" in doc:
                farm_id_value = doc["farm_id"]
                farm_id_str = str(farm_id_value)
                doc["farm_id"] = farm_id_str
            else:
                farm_id_value = None
                farm_id_str = None

            if "farm_polygons_id" in doc:
                doc["farm_polygons_id"] = str(doc["farm_polygons_id"])

            analysis_id_str = None
            if "analysis_id" in doc:
                analysis_id_value = doc["analysis_id"]
                analysis_id_str = str(analysis_id_value)
                doc["analysis_id"] = analysis_id_str

            # ============================
            # Enriquecimiento con Adm3
            # ============================
            department = None
            municipality = None
            vereda = None

            # Recuperar la Farm usando el farm_id original (ObjectId/DBRef)
            if farm_id_value is not None:
                farm = farms_by_id.get(
                    farm_id_value.id if hasattr(farm_id_value, "id") else farm_id_value
                )
            else:
                farm = None

            if farm is not None:
                adm3_id = getattr(farm, "adm3_id", None)
                if adm3_id is not None:
                    adm3 = adm3_by_id.get(adm3_id)
                else:
                    adm3 = None
            else:
                adm3 = None

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

            # Meter el doc en el grupo por analysis_id
            if analysis_id_str is not None and analysis_id_str in grouped_results:
                grouped_results[analysis_id_str].append(doc)
            else:
                # Por si acaso el analysis_id del doc no estaba en la lista original
                grouped_results.setdefault(analysis_id_str or "unknown", []).append(doc)

        return grouped_results

    except HTTPException:
        # Re-lanzar las HTTPException explícitas
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))