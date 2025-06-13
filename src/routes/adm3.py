from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.adm3 import Adm3
from tools.pagination import build_paginated_response, PaginatedResponse

router = APIRouter(
    prefix="/adm3",
    tags=["Admin levels"]
)

class Adm3Schema(BaseModel):
    id: str = Field(..., description="ID interno de MongoDB")
    ext_id: Optional[str] = Field(None, description="ID externo del nivel adm3")
    name: Optional[str] = Field(None, description="Nombre del nivel adm3")
    adm2_id: Optional[str] = Field(None, description="ID del nivel adm2 al que pertenece")
    adm2_name: Optional[str] = Field(None, description="Nombre del nivel adm2 al que pertenece")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "66602cd389f18226a0d9a2aa",
                "ext_id": "5001",
                "name": "MEDELLÍN",
                "adm2_id": "665f1726b1ac3457e3a91a05"
            }
        }


@router.get("/", response_model=List[Adm3Schema])
def get_all_adm3():
    """
    Get all Adm3 records.
    """
    all_adm = Adm3.objects().limit(1000)
    return [serialize_adm3(adm) for adm in all_adm]

@router.get("/by-ids", response_model=List[Adm3Schema])
def get_adm3_by_ids(
    ids: str = Query(..., description="Lista de IDs separados por coma. Ejemplo: ?ids=abc123,def456")
):
    """
    Obtener múltiples registros Adm2 por sus IDs.
    """
    search_ids = [id.strip() for id in ids.split(",") if id.strip()]
    invalid_ids = [i for i in search_ids if not ObjectId.is_valid(i)]
    if invalid_ids:
        raise HTTPException(
            status_code=400,
            detail=f"IDs no válidos: {', '.join(invalid_ids)}"
        )
    matches = Adm3.objects(id__in=search_ids)
    return [serialize_adm3(adm) for adm in matches]

@router.get("/by-name", response_model=List[Adm3Schema])
def get_adm3_by_name(
    name: str = Query(..., description="One or more names (comma-separated) to match partially and case-insensitive")
):
    """
    Get Adm3 records that partially match one or more names.
    Example: /adm3/by-name?name=Cali,Palmira
    """
    search_terms = [term.strip() for term in name.split(",") if term.strip()]
    query = {"$or": [{"name": {"$regex": term, "$options": "i"}} for term in search_terms]}
    matches = Adm3.objects(__raw__=query)
    return [serialize_adm3(adm) for adm in matches]

@router.get("/by-adm2", response_model=List[Adm3Schema])
def get_adm3_by_adm2_ids(
    ids: str = Query(..., description="IDs de Adm1 separados por coma para filtrar registros de Adm3")
):
    """
    Obtener registros Adm3 que pertenezcan a uno o más Adm1 IDs.
    Ejemplo: /adm3/by-adm2?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06
    """
    id_list = [i.strip() for i in ids.split(",") if i.strip()]
    matches = Adm3.objects(adm2_id__in=id_list)
    return [serialize_adm3(adm) for adm in matches]

@router.get("/paged/", response_model=PaginatedResponse[Adm3Schema])
def get_adm3_paginated(
    page: int = Query(1, ge=1, description="Número de página (si se usa paginación clásica)"),
    limit: int = Query(10, ge=1, description="Número máximo de registros por página"),
    skip: Optional[int] = Query(None, ge=0, description="Número de registros a omitir (si se usa tipo scroll)"),
    search: Optional[str] = Query(None, description="Término de búsqueda parcial (case-insensitive) sobre los campos especificados en 'search_fields'."),
    search_fields: Optional[str] = Query(None, description="Campos separados por coma (ej: name,ext_id)"),
    order_by: Optional[str] = Query(None, description="Campo(s) para ordenar, separados por coma. Usa '-' para descendente. Ej: name,-ext_id")
):
    """
    Paginación con búsqueda opcional por varios campos y ordenamiento personalizado.
    """
    base_query = Adm3.objects
    allowed_fields = {"name", "ext_id"}
    fields = [
        f.strip() for f in (search_fields or "name,ext_id").split(",")
        if f.strip() in allowed_fields
    ]

    if search:
        if not fields:
            raise HTTPException(
                status_code=400,
                detail="Debes especificar al menos un campo válido. Opciones: name, ext_id"
            )
        query_parts = [{field: {"$regex": search, "$options": "i"}} for field in fields]
        base_query = base_query(__raw__={"$or": query_parts})

    sort_fields = []
    if order_by:
        for f in order_by.split(","):
            field = f.strip()
            if field.replace("-", "") in allowed_fields:
                sort_fields.append(field)

    return build_paginated_response(
        base_query=base_query,
        schema_model=Adm3Schema,
        page=page,
        limit=limit,
        skip=skip,
        order_by_fields=sort_fields,
        serialize_fn=serialize_adm3
    )

def serialize_adm3(doc):
    return {
        "id": str(doc.id),
        "ext_id": doc.ext_id,
        "name": doc.name,
        "adm2_id": str(doc.adm2_id.id) if doc.adm2_id else None,
        "adm2_name": str(doc.adm2_id.name) if doc.adm2_id else None
    }