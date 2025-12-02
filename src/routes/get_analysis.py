from fastapi import HTTPException, APIRouter, Depends
from bson import ObjectId

from ganabosques_orm.collections.analysis import Analysis
from dependencies.auth_guard import require_admin 

router = APIRouter(
    tags=["Analysis"],
    dependencies=[Depends(require_admin)] 
)

@router.get("/analysis/by-deforestation")
def get_analysis_by_deforestation(deforestation_id: str):
    try:
        if not ObjectId.is_valid(deforestation_id):
            raise HTTPException(status_code=400, detail="ID de deforestación inválido")

        analysis_docs = (
            Analysis.objects(deforestation_id=ObjectId(deforestation_id))
            .select_related()
        )

        if not analysis_docs:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron análisis con ese ID de deforestación"
            )

        resp = []
        for doc in analysis_docs:
            defo = getattr(doc, "deforestation_id", None)

            deforestation_source = (
                str(defo.deforestation_source.value)
                if getattr(defo, "deforestation_source", None)
                else None
            )
            deforestation_type = (
                str(defo.deforestation_type.value)
                if getattr(defo, "deforestation_type", None)
                else None
            )
            deforestation_name = getattr(defo, "name", None) if defo else None
            deforestation_period_start = (
                defo.period_start.isoformat()
                if getattr(defo, "period_start", None)
                else None
            )
            deforestation_period_end = (
                defo.period_end.isoformat()
                if getattr(defo, "period_end", None)
                else None
            )
            deforestation_path = getattr(defo, "path", None) if defo else None

            resp.append({
                "id": str(doc.id),
                "protected_areas_id": (
                    str(doc.protected_areas_id.id)
                    if getattr(doc, "protected_areas_id", None)
                    else None
                ),
                "farming_areas_id": (
                    str(doc.farming_areas_id.id)
                    if getattr(doc, "farming_areas_id", None)
                    else None
                ),
                "deforestation_id": str(defo.id) if defo else None,

                "deforestation_source": deforestation_source,
                "deforestation_type": deforestation_type,
                "deforestation_name": deforestation_name,
                "deforestation_period_start": deforestation_period_start,
                "deforestation_period_end": deforestation_period_end,
                "deforestation_path": deforestation_path,

                "user_id": str(doc.user_id) if getattr(doc, "user_id", None) else None,
                "date": doc.date.isoformat() if getattr(doc, "date", None) else None,
            })

        return resp

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")