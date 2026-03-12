from typing import Type, List, Optional, Callable, Any, Dict, Generic, TypeVar
from pydantic import BaseModel, Field
from pydantic.generics import GenericModel
import time
from tools.utils import convert_doc_to_json

T = TypeVar("T")

class PaginatedResponse(GenericModel, Generic[T]):
    total: int = Field(..., description="Total number of records.")
    limit: int = Field(..., description="Maximum number of records per page.")
    skip: int = Field(..., description="Number of skipped records.")
    page: int = Field(..., description="Current page number.")
    total_pages: int = Field(..., description="Total number of pages.")
    has_next: bool = Field(..., description="Indicates if there is a next page.")
    results: List[T] = Field(..., description="List of results for the current page.")

def build_paginated_response(
    base_query,
    schema_model: Type[BaseModel],
    page: int = 1,
    limit: int = 10,
    skip: Optional[int] = None,
    order_by_fields: Optional[List[str]] = None,
    serialize_fn: Optional[Callable[[Any], Dict]] = None,
):
    """
    Builds a generic paginated response from a MongoEngine query.
    Optimized with as_pymongo() for better performance when no custom serializer is provided.

    Parameters:
    - base_query: MongoEngine QuerySet object.
    - schema_model: Pydantic model class used for output serialization.
    - page: Page number to retrieve (ignored if `skip` is provided).
    - limit: Max number of records per page.
    - skip: Number of records to skip (overrides `page` if provided).
    - order_by_fields: Optional list of fields to order by (MongoEngine syntax).
    - serialize_fn: Optional function to serialize each document. If not provided,
      uses optimized as_pymongo() approach.

    Returns:
    A dictionary with pagination metadata and serialized results,
    ready to be returned as JSON.
    """
    inicio_total = time.perf_counter()
    
    try:
        # Count total (puede ser lento con muchos registros)
        inicio_count = time.perf_counter()
        total = base_query.count()
        fin_count = time.perf_counter()
        
        offset = skip if skip is not None else (page - 1) * limit
        total_pages = (total + limit - 1) // limit
        has_next = (offset + limit) < total
        real_page = page if skip is None else (offset // limit) + 1

        if order_by_fields:
            base_query = base_query.order_by(*order_by_fields).collation({'locale': 'es', 'strength': 1})

        # Query con paginación
        inicio_query = time.perf_counter()
        
        if serialize_fn:
            # Usar serialize_fn custom (más lento pero necesario para lógica específica)
            results = base_query.skip(offset).limit(limit)
            items = [serialize_fn(obj) for obj in results]
        else:
            # Optimización: usar as_pymongo() - mucho más rápido
            docs = list(base_query.skip(offset).limit(limit).as_pymongo())
            
            fin_query = time.perf_counter()
            inicio_serialization = time.perf_counter()
            
            # Conversión recursiva de ObjectIds, fechas y enums
            items = [convert_doc_to_json(doc) for doc in docs]
            
            fin_serialization = time.perf_counter()
            fin_total = time.perf_counter()
            
            # Logging de performance
            print(f"[Pagination] Count: {(fin_count - inicio_count):.3f}s | Query: {(fin_query - inicio_query):.3f}s | Serialization: {(fin_serialization - inicio_serialization):.3f}s | Total: {(fin_total - inicio_total):.3f}s | Page: {real_page}/{total_pages} | Records: {len(items)}")
            
            return {
                "total": total,
                "limit": limit,
                "skip": offset,
                "page": real_page,
                "total_pages": total_pages,
                "has_next": has_next,
                "results": items
            }
        
        fin_query = time.perf_counter()
        fin_total = time.perf_counter()
        
        # Logging cuando usa serialize_fn custom
        print(f"[Pagination Custom] Count: {(fin_count - inicio_count):.3f}s | Query+Serialize: {(fin_query - inicio_query):.3f}s | Total: {(fin_total - inicio_total):.3f}s | Page: {real_page}/{total_pages} | Records: {len(items)}")

        return {
            "total": total,
            "limit": limit,
            "skip": offset,
            "page": real_page,
            "total_pages": total_pages,
            "has_next": has_next,
            "results": items
        }
    
    except Exception as e:
        fin_error = time.perf_counter()
        print(f"[Pagination] ERROR after {(fin_error - inicio_total):.3f}s: {str(e)}")
        # Re-raise para que FastAPI lo maneje
        raise
