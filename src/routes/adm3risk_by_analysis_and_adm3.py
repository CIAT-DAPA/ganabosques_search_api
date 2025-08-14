from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from bson import ObjectId
from ganabosques_orm.collections.adm3risk import Adm3Risk

router = APIRouter()

class Adm3RiskFilterRequest(BaseModel):
    analysis_ids: List[str]
    adm3_ids: List[str]

@router.post("/adm3risk/by-analysis-and-adm3")
def get_adm3risk_filtered(data: Adm3RiskFilterRequest):
    try:
        # Validar ObjectIds
        valid_analysis_ids = []
        valid_adm3_ids = []

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

        # Consultar documentos que coincidan con ambos filtros
        adm3risks = Adm3Risk.objects(
            analysis_id__in=valid_analysis_ids,
            adm3_id__in=valid_adm3_ids
        )

        # Agrupar por analysis_id
        grouped_results: Dict[str, List[Dict[str, Any]]] = {str(a_id): [] for a_id in valid_analysis_ids}

        for risk in adm3risks:
            doc = risk.to_mongo().to_dict()
            doc["_id"] = str(doc["_id"])
            if "analysis_id" in doc:
                doc["analysis_id"] = str(doc["analysis_id"])
            if "adm3_id" in doc:
                doc["adm3_id"] = str(doc["adm3_id"])

            grouped_results[doc["analysis_id"]].append(doc)

        return grouped_results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
