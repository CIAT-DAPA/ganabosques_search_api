# import re
# from fastapi import Query, HTTPException
# from typing import Optional, List
# from pydantic import BaseModel, Field
# from bson import ObjectId
# from ganabosques_orm.collections.enterpriserisk import EnterpriseRisk
# from ganabosques_orm.enums.criteriaenum import CriteriaEnum
# from tools.pagination import build_paginated_response, PaginatedResponse

# from routes.base_route import generate_read_only_router
# from tools.utils import parse_object_ids, build_search_query

# class CriteriaSchema(BaseModel):
#     label: CriteriaEnum = Field(..., description="Criteria label")
#     value: Optional[bool] = Field(None, description="Boolean value for the criterion")

# class EnterpriseRiskSchema(BaseModel):
#     id: str = Field(..., description="MongoDB internal ID of the enterprise risk")
#     enterprise_id: Optional[str] = Field(None, description="ID of the associated enterprise")
#     criteria: List[CriteriaSchema] = Field(default_factory=list, description="List of criteria")
#     risk_total: Optional[float] = Field(None, description="Total calculated risk score")

#     class Config:
#         from_attributes = True
#         json_schema_extra = {
#             "example": {
#                 "id": "6661c001e2ac3457e3a92eee",
#                 "enterprise_id": "665f2222b1ac3457e3a91ccc",
#                 "criteria": [
#                     {"label": "criteria1", "value": True},
#                     {"label": "criteria2", "value": False},
#                     {"label": "criteria3", "value": True}
#                 ],
#                 "risk_total": 1.5
#             }
#         }

# def serialize_enterpriserisk(doc):
#     """Serialize an EnterpriseRisk document into a JSON-compatible dictionary."""
#     return {
#         "id": str(doc.id),
#         "enterprise_id": str(doc.enterprise_id.id) if doc.enterprise_id else None,
#         "criteria": [
#             {
#                 "label": str(c.label.value),
#                 "value": c.value
#             } for c in (doc.criteria or [])
#         ],
#         "risk_total": doc.risk_total
#     }

# router = generate_read_only_router(
#     prefix="/enterpriserisk",
#     tags=["Analysis risk"],
#     collection=EnterpriseRisk,
#     schema_model=EnterpriseRiskSchema,
#     allowed_fields=[],
#     serialize_fn=serialize_enterpriserisk,
#     include_endpoints=["paged"]
# )
