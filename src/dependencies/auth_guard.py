# dependencies/auth_guard.py
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, List
from auth.token_validation_router import validate_local_token
from auth.utils import user_has_permissions

security = HTTPBearer()


def require_token(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """
    Valida que el token JWT sea válido usando validate_local_token.
    No exige ningún rol específico.
    Si el token es inválido o expirado, lanza HTTPException.
    """
    validation_result = validate_local_token(credentials)
    return validation_result 


def require_admin(
    validation_result: dict = Depends(require_token)
):
    """
    Requiere:
    - Token válido (ya garantizado por require_token)
    - Que el usuario tenga admin=True en la base de datos.
    """
    user_db = validation_result["payload"].get("user_db", {})
    
    if not user_db.get("admin", False):
        raise HTTPException(
            status_code=403,
            detail="User is not an administrator"
        )

    return validation_result


def require_permissions(
    required_actions: Optional[List[str]] = None,
    required_options: Optional[List[str]] = None,
    require_all_actions: bool = True,
    require_all_options: bool = True
):
    """
    Dependency factory para requerir permisos específicos.
    Retorna una función de validación que puede ser usada con Depends().
    
    Args:
        required_actions: Lista de acciones requeridas (ej: ["API_FARMS", "API_ENTERPRISE"])
        required_options: Lista de opciones requeridas (ej: ["READ", "CREATE"])
        require_all_actions: Si True, debe tener TODAS las acciones. Si False, al menos una.
        require_all_options: Si True, debe tener TODAS las opciones. Si False, al menos una.
    
    Returns:
        Función de validación para usar con Depends()
    
    Example:
        @router.get("/farms", dependencies=[Depends(require_permissions(
            required_actions=["API_FARMS"],
            required_options=["READ"]
        ))])
        def get_farms(): ...
    """
    def permission_checker(validation_result: dict = Depends(require_token)):
        user_db = validation_result["payload"].get("user_db", {})
        user_ext_id = user_db.get("ext_id")
        
        if not user_ext_id:
            raise HTTPException(status_code=401, detail="User not found")
        
        if not user_has_permissions(
            user_ext_id,
            required_actions=required_actions,
            required_options=required_options,
            require_all_actions=require_all_actions,
            require_all_options=require_all_options
        ):
            raise HTTPException(
                status_code=403,
                detail="Insufficient permissions"
            )
        
        return validation_result
    
    return permission_checker