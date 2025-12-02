from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Dict, Any
from bson import ObjectId
from ganabosques_orm.collections.farmrisk import FarmRisk

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

        grouped_results: Dict[str, List[Dict[str, Any]]] = {str(a_id): [] for a_id in valid_analysis_ids}

        for fr in farmrisks:
            doc = fr.to_mongo().to_dict()
            doc["_id"] = str(doc["_id"])
            if "farm_id" in doc:
                doc["farm_id"] = str(doc["farm_id"])
            if "farm_polygons_id" in doc:
                doc["farm_polygons_id"] = str(doc["farm_polygons_id"])
            if "analysis_id" in doc:
                analysis_id_str = str(doc["analysis_id"])
                doc["analysis_id"] = analysis_id_str
                grouped_results[analysis_id_str].append(doc)

        return grouped_results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))