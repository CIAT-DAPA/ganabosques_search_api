from fastapi import APIRouter, Query, HTTPException
from typing import Type, List, Optional, Callable, Any, Dict
from pydantic import BaseModel
from bson import ObjectId
from tools.pagination import build_paginated_response, PaginatedResponse
from tools.utils import parse_object_ids, build_search_query
import re

def generate_read_only_router(
    *,
    prefix: str,
    tags: List[str],
    collection,
    schema_model: Type[BaseModel],
    allowed_fields: List[str],
    serialize_fn: Optional[Callable[[Any], Dict]] = None,
    include_endpoints: Optional[List[str]] = None 
) -> APIRouter:
    router = APIRouter(prefix=prefix, tags=tags)
    include_endpoints = set(include_endpoints or ["all"])
    pretty_name = re.sub(r'(?<!^)(?=[A-Z])', ' ', collection.__name__).title()

    def serialize(item):
        return serialize_fn(item) if serialize_fn else schema_model(**item.to_mongo().to_dict())
    
    pretty_name = re.sub(r'(?<!^)(?=[A-Z])', ' ', collection.__name__).title()

    @router.get("/", response_model=List[schema_model])
    def get_all(): 
        items = collection.objects()
        return [serialize(i) for i in items]
    get_all.__doc__ = f"Retrieve all {pretty_name} records."

    @router.get("/by-ids", response_model=List[schema_model])
    def get_by_ids(
        ids: str = Query(..., description="Comma-separated list of IDs. Example: ?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06")
    ):
        search_ids = parse_object_ids(ids)
        matches = collection.objects(id__in=search_ids)
        return [serialize(m) for m in matches]
    get_by_ids.__doc__ = f"Retrieve multiple {pretty_name} records by their MongoDB ObjectIds."
    
    if include_endpoints and "by-name" in include_endpoints:
        @router.get("/by-name", response_model=List[schema_model])
        def get_by_name(
            name: str = Query(..., description="One or more comma-separated names for case-insensitive partial search")
        ):
            terms = [term.strip() for term in name.split(",") if term.strip()]
            query = build_search_query(terms, ["name"])
            matches = collection.objects(__raw__=query)
            return [serialize(m) for m in matches]
        get_by_name.__doc__ = f"Search {pretty_name} records by name with partial, case-insensitive match."

    if include_endpoints and "paged" in include_endpoints:
        @router.get("/paged/", response_model=PaginatedResponse[schema_model])
        def get_paginated(
            page: int = Query(1, ge=1, description="Page number to retrieve. Ignored if 'skip' is defined"),
            limit: int = Query(10, ge=1, description="Maximum records per page"),
            skip: Optional[int] = Query(None, ge=0, description="Number of records to skip. Overrides 'page' if defined"),
            search: Optional[str] = Query(None, description="Comma-separated search terms for partial match"),
            search_fields: Optional[str] = Query(None, description=f"Comma-separated fields to search. {', '.join(allowed_fields)}"),
            order_by: Optional[str] = Query(None, description=f"Comma-separated fields to sort. Use '-' for descending {', '.join(allowed_fields)}")
        ):
            base_query = collection.objects

            # Parse fields
            fields = [f.strip() for f in (search_fields or ",".join(allowed_fields)).split(",") if f.strip() in allowed_fields]

            # Search logic
            if search and fields:
                terms = [t.strip() for t in search.split(",") if t.strip()]
                base_query = base_query(__raw__=build_search_query(terms, fields))

            # Order logic
            sort_fields = []
            if order_by:
                for field in order_by.split(","):
                    clean = field.strip().replace("-", "")
                    if clean in allowed_fields:
                        sort_fields.append(field.strip())
                    else:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Invalid sort field: {clean}. Allowed: {', '.join(allowed_fields)}"
                        )

            return build_paginated_response(
                base_query=base_query,
                schema_model=schema_model,
                page=page,
                limit=limit,
                skip=skip,
                order_by_fields=sort_fields,
                serialize_fn=serialize_fn,
            )
        get_paginated.__doc__ = f"Retrieve paginated {pretty_name} records with optional search and sorting."


    return router