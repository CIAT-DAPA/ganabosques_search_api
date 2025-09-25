from typing import Optional
from pydantic import BaseModel, Field
from ganabosques_orm.collections.farmrisk import FarmRisk
from routes.base_route import generate_read_only_router

class AttributeSchema(BaseModel):
    prop: Optional[float] = Field(None, description="Proportion")
    ha: Optional[float] = Field(None, description="Area in hectares")
    distance: Optional[float] = Field(None, description="Distance to feature")

class FarmRiskSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the farm risk")
    farm_id: Optional[str] = Field(None, description="ID of the associated farm")
    analysis_id: Optional[str] = Field(None, description="ID of the associated analysis")
    farm_polygons_id: Optional[str] = Field(None, description="ID of the associated farm polygon")

    # Subdocumentos
    deforestation: Optional[AttributeSchema] = Field(None, description="Deforestation attributes")
    protected: Optional[AttributeSchema] = Field(None, description="Protected area attributes")
    farming_in: Optional[AttributeSchema] = Field(None, description="Attributes for farming_in")
    farming_out: Optional[AttributeSchema] = Field(None, description="Attributes for farming_out")

    # Riesgos booleanos
    risk_direct: Optional[bool] = Field(None, description="Direct risk (>0 -> true, else false)")
    risk_input: Optional[bool] = Field(None, description="Input-based risk (>0 -> true, else false)")
    risk_output: Optional[bool] = Field(None, description="Output-based risk (>0 -> true, else false)")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "68a36b0c12967887c405a8dc",
                "farm_id": "689a803713d2089e5c8b1e2e",
                "analysis_id": "689d1149a6ef4a011d5c394e",
                "farm_polygons_id": "689a803713d2089e5c8b1e2f",
                "deforestation": { "prop": 0, "ha": 0, "distance": 0 },
                "protected": { "prop": 1, "ha": 4.675431693, "distance": 0 },
                "farming_in": { "prop": 0.32, "ha": 1.234, "distance": 12 },
                "farming_out": { "prop": 0.05, "ha": 0.421, "distance": 3 },
                "risk_direct": True,
                "risk_input": False,
                "risk_output": False
            }
        }

def _as_str_oid(value) -> Optional[str]:
    if not value:
        return None
    return str(getattr(value, "id", value))

def _serialize_attr(attr) -> Optional[dict]:
    if not attr:
        return None
    return {
        "prop": getattr(attr, "prop", None),
        "ha": getattr(attr, "ha", None),
        "distance": getattr(attr, "distance", None),
    }

def serialize_farmrisk(doc):
    return {
        "id": str(doc.id),
        "farm_id": _as_str_oid(getattr(doc, "farm_id", None)),
        "analysis_id": _as_str_oid(getattr(doc, "analysis_id", None)),
        "farm_polygons_id": _as_str_oid(getattr(doc, "farm_polygons_id", None)),

        "deforestation": _serialize_attr(getattr(doc, "deforestation", None)),
        "protected": _serialize_attr(getattr(doc, "protected", None)),
        "farming_in": _serialize_attr(getattr(doc, "farming_in", None)),
        "farming_out": _serialize_attr(getattr(doc, "farming_out", None)),

        "risk_direct": getattr(doc, "risk_direct", None),
        "risk_input": getattr(doc, "risk_input", None),
        "risk_output": getattr(doc, "risk_output", None),
    }

router = generate_read_only_router(
    prefix="/farmrisk",
    tags=["Analysis risk"],
    collection=FarmRisk,
    schema_model=FarmRiskSchema,
    allowed_fields=[
        "farm_id", "analysis_id",
        "risk_direct", "risk_input", "risk_output",
    ],
    serialize_fn=serialize_farmrisk,
    include_endpoints=["paged"]
)