# dependencies/auth_guard.py
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from auth.token_validation_router import validate_local_token  # O ajusta el import según tu estructura

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
    - Que el usuario tenga el rol 'admin' en client_roles.
    """
    roles = validation_result["payload"].get("client_roles", [])

    if "Admin" not in roles:
        raise HTTPException(
            status_code=403,
            detail="User does not have the required 'admin' role"
        )

    return validation_result