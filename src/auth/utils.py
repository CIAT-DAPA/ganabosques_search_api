from typing import Union, List, Optional, Dict
from bson import ObjectId
from ganabosques_orm.collections.user import User
from ganabosques_orm.collections.role import Role
from ganabosques_orm.enums.actions import Actions
from ganabosques_orm.enums.options import Options


def get_user_roles(user_identifier: Union[str, ObjectId]) -> List[Dict]:
    """
    Obtiene los roles de un usuario con sus acciones y opciones.
    
    Args:
        user_identifier: Puede ser ext_id (string de Keycloak) o id de MongoDB (ObjectId o string)
    
    Returns:
        Lista de diccionarios con estructura: 
        [{"name": "rol_name", "actions": [...], "options": [...]}]
    """
    # Buscar usuario por ext_id o por _id
    if isinstance(user_identifier, str) and len(user_identifier) == 24:
        try:
            user = User.objects(id=ObjectId(user_identifier)).first()
        except:
            user = User.objects(ext_id=user_identifier).first()
    else:
        user = User.objects(ext_id=str(user_identifier)).first()
    
    if not user:
        return []
    
    if not user.role:
        return []
    
    roles_data = []
    for role_ref in user.role:
        role = Role.objects(id=role_ref.id).first()
        if role:
            roles_data.append({
                "id": str(role.id),
                "name": role.name,
                "actions": [action.value for action in (role.actions or [])],
                "options": [option.value for option in (role.options or [])]
            })
    
    return roles_data


def get_user_by_identifier(user_identifier: Union[str, ObjectId]) -> Optional[User]:
    """
    Obtiene un usuario por ext_id o id de MongoDB.
    
    Args:
        user_identifier: ext_id (string de Keycloak) o id de MongoDB
    
    Returns:
        Objeto User o None si no existe
    """
    if isinstance(user_identifier, str) and len(user_identifier) == 24:
        try:
            return User.objects(id=ObjectId(user_identifier)).first()
        except:
            return User.objects(ext_id=user_identifier).first()
    else:
        return User.objects(ext_id=str(user_identifier)).first()


def user_is_admin(user_identifier: Union[str, ObjectId]) -> bool:
    """
    Verifica si un usuario es administrador.
    
    Args:
        user_identifier: ext_id o id de MongoDB
    
    Returns:
        True si es admin, False en caso contrario
    """
    user = get_user_by_identifier(user_identifier)
    return user.admin if user and user.admin else False


def user_has_permissions(
    user_identifier: Union[str, ObjectId],
    required_actions: Optional[List[Union[str, Actions]]] = None,
    required_options: Optional[List[Union[str, Options]]] = None,
    require_all_actions: bool = True,
    require_all_options: bool = True
) -> bool:
    """
    Verifica si un usuario tiene los permisos requeridos.
    
    Args:
        user_identifier: ext_id o id de MongoDB
        required_actions: Lista de acciones requeridas (puede ser strings o enums)
        required_options: Lista de opciones requeridas (puede ser strings o enums)
        require_all_actions: Si True, debe tener TODAS las acciones. Si False, al menos una.
        require_all_options: Si True, debe tener TODAS las opciones. Si False, al menos una.
    
    Returns:
        True si tiene los permisos, False en caso contrario
    
    Examples:
        >>> user_has_permissions("user_id", required_actions=["API_FARMS"], required_options=["READ"])
        >>> user_has_permissions("user_id", required_actions=[Actions.API_FARMS], require_all_actions=False)
    """
    user = get_user_by_identifier(user_identifier)
    
    if not user:
        return False
    
    # Si es admin, tiene todos los permisos
    if user.admin:
        return True
    
    # Obtener todos los permisos del usuario
    roles = get_user_roles(user_identifier)
    
    if not roles:
        return False
    
    # Consolidar todas las acciones y opciones de todos los roles
    user_actions = set()
    user_options = set()
    
    for role in roles:
        user_actions.update(role.get("actions", []))
        user_options.update(role.get("options", []))
    
    # Convertir enums a strings para comparación
    if required_actions:
        required_actions_str = [
            action.value if isinstance(action, Actions) else action 
            for action in required_actions
        ]
        
        if require_all_actions:
            if not all(action in user_actions for action in required_actions_str):
                return False
        else:
            if not any(action in user_actions for action in required_actions_str):
                return False
    
    if required_options:
        required_options_str = [
            option.value if isinstance(option, Options) else option 
            for option in required_options
        ]
        
        if require_all_options:
            if not all(option in user_options for option in required_options_str):
                return False
        else:
            if not any(option in user_options for option in required_options_str):
                return False
    
    return True


def user_has_action(user_identifier: Union[str, ObjectId], action: Union[str, Actions]) -> bool:
    """
    Verifica si un usuario tiene una acción específica.
    
    Args:
        user_identifier: ext_id o id de MongoDB
        action: Acción a verificar (string o enum)
    
    Returns:
        True si tiene la acción, False en caso contrario
    """
    return user_has_permissions(user_identifier, required_actions=[action])


def user_has_option(user_identifier: Union[str, ObjectId], option: Union[str, Options]) -> bool:
    """
    Verifica si un usuario tiene una opción específica.
    
    Args:
        user_identifier: ext_id o id de MongoDB
        option: Opción a verificar (string o enum)
    
    Returns:
        True si tiene la opción, False en caso contrario
    """
    return user_has_permissions(user_identifier, required_options=[option])


def get_user_actions(user_identifier: Union[str, ObjectId]) -> List[str]:
    """
    Obtiene todas las acciones únicas de un usuario.
    
    Args:
        user_identifier: ext_id o id de MongoDB
    
    Returns:
        Lista de strings con las acciones
    """
    roles = get_user_roles(user_identifier)
    actions = set()
    for role in roles:
        actions.update(role.get("actions", []))
    return list(actions)


def get_user_options(user_identifier: Union[str, ObjectId]) -> List[str]:
    """
    Obtiene todas las opciones únicas de un usuario.
    
    Args:
        user_identifier: ext_id o id de MongoDB
    
    Returns:
        Lista de strings con las opciones
    """
    roles = get_user_roles(user_identifier)
    options = set()
    for role in roles:
        options.update(role.get("options", []))
    return list(options)


def serialize_user_permissions(user_identifier: Union[str, ObjectId]) -> Dict:
    """
    Serializa toda la información de permisos de un usuario.
    
    Args:
        user_identifier: ext_id o id de MongoDB
    
    Returns:
        Diccionario con estructura completa de permisos
    """
    user = get_user_by_identifier(user_identifier)
    
    if not user:
        return {
            "id": None,
            "ext_id": None,
            "admin": False,
            "roles": [],
            "all_actions": [],
            "all_options": []
        }
    
    roles = get_user_roles(user_identifier)
    
    return {
        "id": str(user.id),
        "ext_id": user.ext_id,
        "admin": user.admin if user.admin else False,
        "roles": roles,
        "all_actions": get_user_actions(user_identifier),
        "all_options": get_user_options(user_identifier)
    }
