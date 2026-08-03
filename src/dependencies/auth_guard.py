# dependencies/auth_guard.py
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, List
from src.auth.token_validation_router import validate_local_token
from src.auth.utils import user_has_permissions

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
    permissions: List[dict],
    require_all: bool = False,
):
    """
    Requiere varios permisos.

    permissions = [

        {
            "actions": PermissionGroups.FARMS,
            "option": Options.READ
        },

        {
            "actions": PermissionGroups.ENTERPRISE,
            "option": Options.UPDATE
        }

    ]

    require_all=True

        Debe cumplir todos.

    require_all=False

        Debe cumplir al menos uno.
    """

    def permission_checker(
        validation_result: dict = Depends(require_token)
    ):

        user_db = validation_result["payload"].get("user_db", {})

        user_ext_id = user_db.get("ext_id")

        if not user_ext_id:
            raise HTTPException(
                status_code=401,
                detail="User not found"
            )
        #print(f"[require_permissions] User: {user_db}, Permissions: {permissions}, Require all: {require_all}")

        if not user_has_permissions(
            user_db,
            permissions,
            require_all
        ):
            raise HTTPException(
                status_code=403,
                detail="Insufficient permissions"
            )
        return validation_result
    return permission_checker