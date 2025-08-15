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

        analysis_docs = Analysis.objects(deforestation_id=ObjectId(deforestation_id)).select_related()

        if not analysis_docs:
            raise HTTPException(status_code=404, detail="No se encontraron análisis con ese ID de deforestación")

        return [
            {
                "id": str(doc.id),
                "protected_areas_id": str(doc.protected_areas_id.id) if doc.protected_areas_id else None,
                "farming_areas_id": str(doc.farming_areas_id.id) if doc.farming_areas_id else None,
                "deforestation_id": str(doc.deforestation_id.id) if doc.deforestation_id else None,
                "deforestation_source": str(doc.deforestation_id.deforestation_source.value) if doc.deforestation_id.deforestation_source else None,
                "deforestation_type": str(doc.deforestation_id.deforestation_type.value) if doc.deforestation_id.deforestation_type else None,
                "deforestation_name": str(doc.deforestation_id.name) if doc.deforestation_id.name else None,
                "deforestation_year_start": str(doc.deforestation_id.year_start) if doc.deforestation_id.year_start else None,
                "deforestation_year_end": str(doc.deforestation_id.year_end) if doc.deforestation_id.year_end else None,
                "deforestation_path": str(doc.deforestation_id.path) if doc.deforestation_id else None,
                "user_id": str(doc.user_id) if doc.user_id else None,
                "date": doc.date.isoformat() if doc.date else None
            }
            for doc in analysis_docs
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")
