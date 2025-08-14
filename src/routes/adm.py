from fastapi import APIRouter
from typing import List
from ganabosques_orm.collections.adm1 import Adm1
from ganabosques_orm.collections.adm2 import Adm2
from ganabosques_orm.collections.adm3 import Adm3

router = APIRouter(
    prefix="/adm",
    tags=["Admin levels"]
)

@router.get("/", summary="Get combined Adm1, Adm2, Adm3", response_model=List[dict])
def get_all_adm():
    """
    Returns a unified list of Adm1, Adm2, and Adm3 combinations.
    Each entry includes:
    - adm1_id
    - adm1_name
    - adm2_id
    - adm2_name
    - adm3_id
    - adm3_name
    - label: "<ADM1_NAME>, <ADM2_NAME>, <ADM3_NAME>"
    """
    result = []

    # Create lookups to reduce DB hits
    adm1_lookup = {str(adm.id): adm.name for adm in Adm1.objects()}
    adm2_lookup = {str(adm.id): {"name": adm.name, "adm1_id": str(adm.adm1_id.id) if adm.adm1_id else None} for adm in Adm2.objects()}

    # Iterate over Adm3 and join with Adm2 and Adm1
    for adm3 in Adm3.objects():
        adm2_id = str(adm3.adm2_id.id) if adm3.adm2_id else None
        adm2_info = adm2_lookup.get(adm2_id, {})
        adm2_name = adm2_info.get("name", "UNKNOWN")
        adm1_id = adm2_info.get("adm1_id")
        adm1_name = adm1_lookup.get(adm1_id, "UNKNOWN")

        result.append({
            "adm1_id": adm1_id,
            "adm1_name": adm1_name,
            "adm2_id": adm2_id,
            "adm2_name": adm2_name,
            "adm3_id": str(adm3.id),
            "adm3_name": adm3.name,
            "label": f"{adm1_name}, {adm2_name}, {adm3.name}"
        })

    return result