import re
from fastapi import Query, HTTPException, Depends, APIRouter
from typing import Optional, List, Union
from pydantic import BaseModel, Field
from datetime import datetime

from ganabosques_orm.collections.analysis import Analysis
from routes.base_route import generate_read_only_router
from dependencies.auth_guard import require_token


class AnalysisSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the analysis")
    protected_areas_id: Optional[str] = Field(None, description="ID of the referenced ProtectedArea document")
    farming_areas_id: Optional[str] = Field(None, description="ID of the referenced FarmingArea document")
    deforestation_id: Optional[str] = Field(None, description="ID of the referenced Deforestation document")
    deforestation_source: Optional[str] = Field(None, description="Source of the deforestation data (e.g., SMBYC)")
    deforestation_type: Optional[str] = Field(None, description="Type of deforestation data: annual or cumulative")
    deforestation_name: Optional[str] = Field(None, description="Name or label for the deforestation data")
    deforestation_period_start: Optional[datetime] = Field(None, description="Start datetime of the deforestation window")
    deforestation_period_end: Optional[datetime] = Field(None, description="End datetime of the deforestation window")
    deforestation_path: Optional[str] = Field(None, description="Path or reference to the deforestation file in Geoserver")
    user_id: Optional[str] = Field(None, description="ID of the user who created the analysis")
    date: Optional[datetime] = Field(None, description="Datetime when the analysis was created")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "6660aaaab1ac3457e3a91f88",
                "protected_areas_id": "665faaaab1ac3457e3a91f01",
                "farming_areas_id": "665fbbbcb1ac3457e3a91f11",
                "deforestation_id": "665fcccdb1ac3457e3a91f22",
                "deforestation_source": "smbyc",
                "deforestation_type": "annual",
                "deforestation_name": "smbyc_deforestation_annual_2010_2012",
                "deforestation_period_start": "2010-01-01T00:00:00+00:00",
                "deforestation_period_end": "2012-12-31T23:59:59+00:00",
                "deforestation_path": "deforestation/smbyc_deforestation_annual/",
                "user_id": "664f1234b1ac3457e3a90009",
                "date": "2025-06-11T14:30:00Z"
            }
        }


def _enum_or_str(val):
    if val is None:
        return None
    return str(getattr(val, "value", val))


def _to_dt_or_none(val: Union[datetime, str, int, None]) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _safe_period_end(a) -> datetime:
    denorm = _to_dt_or_none(getattr(a, "deforestation_period_end", None))
    if denorm is not None:
        return denorm

    d = getattr(a, "deforestation_id", None)
    if d is not None:
        pend = _to_dt_or_none(getattr(d, "period_end", None))
        if pend is not None:
            return pend

    return datetime.min


def serialize_analysis(doc):
    d = getattr(doc, "deforestation_id", None)

    def_source = None
    def_type = None
    def_name = None
    def_period_start = None
    def_period_end = None
    def_path = None

    if d:
        def_source = _enum_or_str(getattr(d, "deforestation_source", None))
        def_type = _enum_or_str(getattr(d, "deforestation_type", None))
        def_name = getattr(d, "name", None)
        def_period_start = _to_dt_or_none(getattr(d, "period_start", None))
        def_period_end = _to_dt_or_none(getattr(d, "period_end", None))
        def_path = getattr(d, "path", None)

    return {
        "id": str(doc.id),
        "protected_areas_id": str(doc.protected_areas_id.id) if getattr(doc, "protected_areas_id", None) else None,
        "farming_areas_id": str(doc.farming_areas_id.id) if getattr(doc, "farming_areas_id", None) else None,
        "deforestation_id": str(d.id) if d else None,
        "deforestation_source": def_source,
        "deforestation_type": def_type,
        "deforestation_name": str(def_name) if def_name is not None else None,
        "deforestation_period_start": def_period_start,
        "deforestation_period_end": def_period_end,
        "deforestation_path": str(def_path) if def_path is not None else None,
        "user_id": str(doc.user_id) if getattr(doc, "user_id", None) else None,
        "date": doc.date.isoformat() if getattr(doc, "date", None) else None,
    }


_inner_router = generate_read_only_router(
    prefix="/analysis",
    tags=["Analysis risk"],
    collection=Analysis,
    schema_model=AnalysisSchema,
    allowed_fields=[],
    serialize_fn=serialize_analysis,
    include_endpoints=["paged"],
    include_get_all=False
)


@_inner_router.get("/", response_model=List[AnalysisSchema])
def get_all():
    items = Analysis.objects.select_related()
    items_sorted = sorted(items, key=_safe_period_end, reverse=True)
    return [serialize_analysis(i) for i in items_sorted]


router = APIRouter(
    dependencies=[Depends(require_token)]
)

router.include_router(_inner_router)