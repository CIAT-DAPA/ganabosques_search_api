from fastapi import APIRouter, Query, HTTPException
from typing import Type, List, Optional, Callable, Any, Dict
from pydantic import BaseModel
from bson import ObjectId
from tools.pagination import build_paginated_response, PaginatedResponse
from tools.utils import parse_object_ids, build_search_query
from importlib import import_module
from enum import Enum
from typing import List
import re

router = APIRouter(
    prefix="/enums",
    tags=["Enums"]
)

@router.get("/", response_model=List[str], summary="Get enumeration values", response_description="A list of enumeration values")
def read_enum(enum_name: str) -> List[str]:
    """Retrieve the list of values for the requested enumeration.

    This endpoint supports flexible capitalization. You may specify either 
    the lowercase form (e.g., ``/enums/species``) or the exact class name 
    (e.g., ``/enums/Species`` or ``/enums/DeforestationSource``). If the name
    is compound (has multiple words), each word after the first should start
    with a capital letter (PascalCase).

    Parameters
    ----------
    enum_name: str
        The name of the enumeration to look up. Casing is flexible, but for compound
        names, use PascalCase.

    Returns
    -------
    List[str]
        A list of the enumeration's ``value`` fields.
    """
    # Normalise the incoming name for case insensitive lookup
    try:
        module_path = f"ganabosques_orm.enums.{enum_name.lower()}"     # Ej: my_orm.enums.species
        class_name = enum_name[0].upper() + enum_name[1:]      # Ej: Species

        module = import_module(module_path)
        enum_class = getattr(module, class_name)
        if not issubclass(enum_class, Enum):
            raise TypeError
        return [member.value for member in enum_class]
    except ModuleNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Failed to import module '{module_path}': {exc}")
    except AttributeError:
        raise HTTPException(status_code=404, detail=f"Enum class '{class_name}' not found in module '{module_path}'")
    except TypeError:
        raise HTTPException(status_code=400, detail=f"Object '{class_name}' in module '{module_path}' is not an Enum")