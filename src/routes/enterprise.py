import re
from fastapi import Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.enterprise import Enterprise
from tools.pagination import build_paginated_response, PaginatedResponse
from schemas.logschema import LogSchema
import math

from routes.base_route import generate_read_only_router

from datetime import datetime
from ganabosques_orm.enums.typeenterprise import TypeEnterprise
from ganabosques_orm.enums.label import Label

class ExtIdEnterpriseSchema(BaseModel):
    label: Label = Field(..., description="Label type for the external ID")
    ext_code: str = Field(..., description="External code associated with the label")

class EnterpriseSchema(BaseModel):
    id: str = Field(..., description="Internal MongoDB ID of the enterprise")
    adm2_id: Optional[str] = Field(None, description="ID of the associated Adm2 document")
    name: Optional[str] = Field(None, description="Name of the enterprise")
    ext_id: List[ExtIdEnterpriseSchema] = Field(..., description="List of external identifiers")
    type_enterprise: TypeEnterprise = Field(..., description="Type of the enterprise (e.g., SLAUGHTERHOUSE, COLLECTION_CENTER)")
    latitude: Optional[float] = Field(None, description="Latitude of the enterprise location")
    longitud: Optional[float] = Field(None, description="Longitude of the enterprise location")
    log: Optional[LogSchema] = Field(None, description="Logging metadata")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "665f1726b1ac3457e3a91a02",
                "adm2_id": "664f2222b1ac3457e3a90001",
                "name": "Centro de Acopio El Roble",
                "ext_id": [
                    {
                        "label": "PRODUCTIONUNIT_ID",
                        "ext_code": "CA-2024-007"
                    }
                ],
                "type_enterprise": "COLLECTION_CENTER",
                "latitude": 3.4516,
                "longitud": -76.5320,
                "log": {
                    "enable": True,
                    "created": "2024-02-20T09:30:00Z",
                    "updated": "2025-06-10T11:45:00Z"
                }
            }
        }

def serialize_enterprise(doc):
    """Serialize an Enterprise document into a JSON-compatible dictionary."""
    return {
        "id": str(doc.id),
        "adm2_id": str(doc.adm2_id.id) if doc.adm2_id else None,
        "name": doc.name,
        "ext_id": [
            {
                "label": ext.label.value if ext.label else None,
                "ext_code": ext.ext_code
            } for ext in (doc.ext_id or [])
        ],
        "type_enterprise": doc.type_enterprise.value if doc.type_enterprise else None,
        "latitude": safe_float(doc.latitude),
        "longitud": safe_float(doc.longitud),
        "log": {
            "enable": doc.log.enable if doc.log else None,
            "created": doc.log.created.isoformat() if doc.log and doc.log.created else None,
            "updated": doc.log.updated.isoformat() if doc.log and doc.log.updated else None
        } if doc.log else None
    }

router = generate_read_only_router(
    prefix="/enterprise",
    tags=["Farm and Enterprise"],
    collection=Enterprise,
    schema_model=EnterpriseSchema,
    allowed_fields=["name", "type_enterprise"],
    serialize_fn=serialize_enterprise,
    include_endpoints=["paged","by-name", "by-extid"]
)

def safe_float(value):
    """Devuelve None si el valor no es un float válido para JSON."""
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value

@router.get("/by-adm2", response_model=List[EnterpriseSchema])
def get_enterprise_by_adm2_ids(
    ids: str = Query(..., description="Comma-separated Adm2 IDs to filter Enterprises records")
):
    """
    Retrieve Enterprise records that belong to one or more Adm2 IDs.
    Example: /by-adm2?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06
    """
    search_ids = [id.strip() for id in ids.split(",") if id.strip()]
    invalid_ids = [i for i in search_ids if not ObjectId.is_valid(i)]
    if invalid_ids:
        raise HTTPException(
            status_code=400,
            detail=f"IDs no válidos: {', '.join(invalid_ids)}"
        )
    matches = Enterprise.objects(adm3_id__in=search_ids)
    return [serialize_enterprise(enterprise) for enterprise in matches]

# valid_labels_str = ", ".join([l.name for l in Label])

# @router.get("/by-extid", response_model=List[EnterpriseSchema])
# def get_enterprise_by_extid(
#     ext_codes: Optional[str] = Query(
#         None,
#         description="Comma-separated ext_code values to filter in ext_id.ext_code"
#     ),
#     labels: Optional[str] = Query(
#         None,
#         description=f"Comma-separated label values to filter in ext_id.label. Valid options: {valid_labels_str}"
#     )
# ):
#     """
#     Retrieve Enterprise records that match one or more ext_id.ext_code or ext_id.label values.
#     At least one of the two parameters must be provided.

#     Examples:
#     - /enterprise/by-ext-id?ext_codes=CA-2024-007
#     - /enterprise/by-ext-id?labels=PRODUCTIONUNIT_ID
#     - /enterprise/by-ext-id?ext_codes=CA-2024-007&labels=PRODUCTIONUNIT_ID
#     """
#     if not ext_codes and not labels:
#         raise HTTPException(
#             status_code=400,
#             detail="At least one of 'ext_codes' or 'labels' must be provided."
#         )

#     query_conditions = []

#     if ext_codes:
#         ext_code_list = [re.escape(code.strip()) for code in ext_codes.split(",") if code.strip()]
#         query_conditions.append({"ext_id": {"$elemMatch": {"ext_code": {"$in": ext_code_list}}}})

#     if labels:
#         raw_labels = [label.strip() for label in labels.split(",") if label.strip()]
#         invalid_labels = [l for l in raw_labels if l not in Label.__members__]
#         if invalid_labels:
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"Invalid label(s): {', '.join(invalid_labels)}. "
#                        f"Valid options: {valid_labels_str}"
#             )
#         enum_labels = [re.escape(Label[l].value) for l in raw_labels]
#         query_conditions.append({"ext_id": {"$elemMatch": {"label": {"$in": enum_labels}}}})

#     query = {"$and": query_conditions} if len(query_conditions) == 2 else query_conditions[0]

#     enterprises = Enterprise.objects(__raw__=query)
#     return [serialize_enterprise(e) for e in enterprises]
@router.get("/by-name", response_model=List[EnterpriseSchema])
def get_enterprise_by_name(
    name: str = Query(..., description="Uno o más nombres de empresa (comma-separated) para búsqueda parcial case-insensitive")
):
    """
    Busca enterprises por su nombre (partial, case-insensitive).
    Ejemplo: /enterprise/by-name?name=frigorifico,ganaderia
    """
    # 1) Parsear términos
    terms = [t.strip() for t in name.split(",") if t.strip()]
    if not terms:
        raise HTTPException(status_code=400, detail="Debes proporcionar al menos un término en 'name'.")

    # 2) Construir query de regex con OR
    ors = [{"name": {"$regex": re.escape(t), "$options": "i"}} for t in terms]
    query = {"$or": ors}

    # 3) Buscar y devolver
    matches = Enterprise.objects(__raw__=query)
    return [serialize_enterprise(e) for e in matches]

    