from fastapi import FastAPI, HTTPException, APIRouter
from bson import ObjectId
from mongoengine import connect
from ganabosques_orm.collections.analysis import Analysis

# Conectar a MongoDB

# Instancia de FastAPI

router = APIRouter()

@router.get("/farmrisk/by-analysis-and-farm")
def get_analysis_by_deforestation(deforestation_id: str):
    try:
        if not ObjectId.is_valid(deforestation_id):
            raise HTTPException(status_code=400, detail="ID de deforestación inválido")

        analysis_docs = Analysis.objects(deforestation_id=ObjectId(deforestation_id))

        if not analysis_docs:
            raise HTTPException(status_code=404, detail="No se encontraron análisis con ese ID de deforestación")

        return [
            {
                "id": str(doc.id),
                "protected_areas_id": str(doc.protected_areas_id.id) if doc.protected_areas_id else None,
                "farming_areas_id": str(doc.farming_areas_id.id) if doc.farming_areas_id else None,
                "deforestation_id": str(doc.deforestation_id.id),
                "user_id": str(doc.user_id),
                "date": doc.date.isoformat() if doc.date else None
            }
            for doc in analysis_docs
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")
