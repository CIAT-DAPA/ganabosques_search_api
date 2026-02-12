from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional
from bson import ObjectId
from datetime import datetime

from ganabosques_orm.collections.farmriskverification import FarmRiskVerification
from ganabosques_orm.collections.userverifier import UserVerifier
from ganabosques_orm.collections.farmrisk import FarmRisk
from ganabosques_orm.collections.user import User
from dependencies.auth_guard import require_token

router = APIRouter(
    prefix="/farmriskverification",
    tags=["Farm Risk"]
)


class FarmRiskVerificationCreateRequest(BaseModel):
    farmrisk_id: str = Field(..., description="ID del FarmRisk a verificar")
    observation: Optional[str] = Field(None, description="Observaciones de la verificación")
    status: bool = Field(..., description="Estado de la verificación (True=confirmado, False=rechazado)")

    class Config:
        json_schema_extra = {
            "example": {
                "farmrisk_id": "665f1726b1ac3457e3a91a05",
                "observation": "Verificado en campo, alerta confirmada",
                "status": True
            }
        }


class FarmRiskVerificationResponse(BaseModel):
    id: str = Field(..., description="ID de la verificación creada")
    user_id: str = Field(..., description="ID del usuario que creó la verificación")
    farmrisk_id: str = Field(..., description="ID del FarmRisk verificado")
    verification_date: str = Field(..., description="Fecha y hora de la verificación")
    observation: Optional[str] = Field(None, description="Observaciones")
    status: bool = Field(..., description="Estado de la verificación")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "665f1726b1ac3457e3a91a99",
                "user_id": "665f1726b1ac3457e3a91a10",
                "farmrisk_id": "665f1726b1ac3457e3a91a05",
                "verification_date": "2026-02-10T14:30:00",
                "observation": "Verificado en campo, alerta confirmada",
                "status": True
            }
        }


@router.post("/", response_model=FarmRiskVerificationResponse)
def create_farmrisk_verification(
    data: FarmRiskVerificationCreateRequest,
    validation_result: dict = Depends(require_token)
):
    """
    Crea una nueva verificación de FarmRisk.
    
    El usuario se obtiene del token validado y la fecha se establece automáticamente.
    """
    try:
        # Validar farmrisk_id
        if not ObjectId.is_valid(data.farmrisk_id):
            raise HTTPException(status_code=400, detail=f"Invalid farmrisk_id: {data.farmrisk_id}")
        
        farmrisk_id = ObjectId(data.farmrisk_id)
        
        # Verificar que el FarmRisk existe
        farmrisk = FarmRisk.objects(id=farmrisk_id).first()
        if not farmrisk:
            raise HTTPException(status_code=404, detail=f"FarmRisk not found: {data.farmrisk_id}")
        
        # Obtener user_id del token validado
        user_db = validation_result["payload"].get("user_db", {})
        user_mongo_id = user_db.get("id")
        
        if not user_mongo_id:
            raise HTTPException(status_code=401, detail="User ID not found in token")
        
        # Verificar que el usuario existe
        user = User.objects(id=ObjectId(user_mongo_id)).first()
        if not user:
            raise HTTPException(status_code=404, detail=f"User not found: {user_mongo_id}")
        
        userverrification = UserVerifier.objects(id=ObjectId(user_mongo_id)).first()

        if not userverrification:
            raise HTTPException(status_code=403, detail=f"User not have verification rights: {user_mongo_id}")
        
        # Crear la verificación con la fecha actual
        verification_date = datetime.now()
        
        new_verification = FarmRiskVerification(
            user_id=user,
            farmrisk=farmrisk,
            verification=verification_date,
            observation=data.observation,
            status=data.status
        )

        new_verification.save()
        
        # Retornar respuesta
        return FarmRiskVerificationResponse(
            id=str(new_verification.id),
            user_id=str(user.id),
            farmrisk_id=str(farmrisk.id),
            verification_date=verification_date.isoformat(),
            observation=new_verification.observation,
            status=new_verification.status
        )
    
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
