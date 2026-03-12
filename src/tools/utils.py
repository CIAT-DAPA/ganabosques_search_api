import re
from fastapi import HTTPException
from bson import ObjectId
from typing import List, Dict, Any
from datetime import datetime

def parse_object_ids(ids_str: str) -> List[str]:
    """Parse and validate a comma-separated string of ObjectIds."""
    ids = [id.strip() for id in ids_str.split(",") if id.strip()]
    invalid = [i for i in ids if not ObjectId.is_valid(i)]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid ObjectIds: {', '.join(invalid)}"
        )
    return ids

def build_search_query(terms: List[str], fields: List[str]) -> dict:
    """Construct a raw MongoDB OR query using regex for partial match."""
    safe_terms = [re.escape(term.strip()) for term in terms if term.strip()]
    return {
        "$or": [
            {field: {"$regex": term, "$options": "i"}}
            for term in safe_terms
            for field in fields
        ]
    }

def convert_doc_to_json(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively convert MongoDB document to JSON-serializable format.
    Handles:
    - ObjectId -> string
    - datetime -> ISO format string
    - Enums -> value
    - BSON types (Int64, etc.) -> native Python types
    - None values -> None (preserves null)
    - Nested dicts and lists
    """
    # Manejar None primero
    if doc is None:
        return None
    
    if isinstance(doc, dict):
        result = {}
        for key, value in doc.items():
            # Convertir _id a id
            if key == "_id":
                result["id"] = str(value) if value is not None else None
            else:
                result[key] = convert_doc_to_json(value)
        return result
    
    elif isinstance(doc, list):
        return [convert_doc_to_json(item) for item in doc]
    
    elif isinstance(doc, ObjectId):
        return str(doc)
    
    elif isinstance(doc, datetime):
        return doc.isoformat()
    
    elif hasattr(doc, 'value'):  # Es un Enum
        return doc.value
    
    # Manejar tipos especiales de BSON
    elif type(doc).__name__ == 'Int64':
        return int(doc)
    
    elif type(doc).__name__ == 'Decimal128':
        return float(str(doc))
    
    # Manejar float con NaN o Infinity (no válidos en JSON)
    elif isinstance(doc, float):
        import math
        if math.isnan(doc) or math.isinf(doc):
            return None
        return doc
    
    else:
        return doc
