from fastapi import FastAPI, HTTPException, APIRouter
from bson import ObjectId
from ganabosques_orm.collections.analysis import Analysis

router = APIRouter()

@router.get("/farmrisk/by-analysis-and-farm")
def get_analysis_by_deforestation(deforestation_id: str):
    try:
        if not ObjectId.is_valid(deforestation_id):
            raise HTTPException(status_code=400, detail="ID de deforestación inválido")

        # Si quieres evitar múltiples queries por cada doc, usa select_related (deja el deref activo)
        analysis_docs = (
            Analysis.objects(deforestation_id=ObjectId(deforestation_id))
            .select_related()  # trae el doc referenciado para doc.deforestation_id
        )

        if not analysis_docs:
            raise HTTPException(status_code=404, detail="No se encontraron análisis con ese ID de deforestación")

        resp = []
        for doc in analysis_docs:
            # defo puede ser None si no dereferenció; lo tratamos con cuidado
            defo = getattr(doc, "deforestation_id", None)

            # Campos de deforestation (con period_* en vez de year_*)
            deforestation_source = (
                str(defo.deforestation_source.value) if getattr(defo, "deforestation_source", None) else None
            )
            deforestation_type = (
                str(defo.deforestation_type.value) if getattr(defo, "deforestation_type", None) else None
            )
            deforestation_name = getattr(defo, "name", None) if defo else None
            deforestation_period_start = (
                defo.period_start.isoformat() if getattr(defo, "period_start", None) else None
            )
            deforestation_period_end = (
                defo.period_end.isoformat() if getattr(defo, "period_end", None) else None
            )
            deforestation_path = getattr(defo, "path", None) if defo else None

            resp.append({
                "id": str(doc.id),
                "protected_areas_id": str(doc.protected_areas_id.id) if getattr(doc, "protected_areas_id", None) else None,
                "farming_areas_id": str(doc.farming_areas_id.id) if getattr(doc, "farming_areas_id", None) else None,
                "deforestation_id": str(defo.id) if defo else None,

                # ⬇️ period_* en lugar de year_*
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
        # No expongas trazas internas; mensaje claro y conciso
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")