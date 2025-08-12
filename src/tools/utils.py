import re
from fastapi import HTTPException
from bson import ObjectId
from typing import List

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
