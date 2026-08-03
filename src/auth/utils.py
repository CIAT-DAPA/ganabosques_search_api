from typing import Union, List, Optional, Dict
from bson import ObjectId

from ganabosques_orm.collections.user import User
from ganabosques_orm.enums.actions import Actions
from ganabosques_orm.enums.options import Options


def _build_permission_index(user: User) -> Dict:
    """
    Construye un índice de permisos del usuario.
    """

    index = {
        "admin": bool(user.admin),
        "roles": [],
        "actions": set(),
        "permissions": {},
    }

    if not user.role:
        return index

    for role in user.role:

        serialized_role = {
            "id": str(role.id),
            "name": role.name,
            "actions": [],
        }

        for permission in role.actions or []:

            action = permission.action.value

            options = {option.value for option in (permission.options or [])}

            serialized_role["actions"].append(
                {
                    "action": action,
                    "options": sorted(options),
                }
            )

            index["actions"].add(action)

            if action not in index["permissions"]:
                index["permissions"][action] = set()

            index["permissions"][action].update(options)

        index["roles"].append(serialized_role)

    return index


def get_user_by_identifier(user_identifier: Union[str, ObjectId]) -> Optional[User]:
    """
    Obtiene un usuario por ext_id o id Mongo.
    """

    if isinstance(user_identifier, ObjectId):
        return User.objects(id=user_identifier).first()

    if isinstance(user_identifier, str) and len(user_identifier) == 24:
        try:
            return User.objects(id=ObjectId(user_identifier)).first()
        except Exception:
            pass

    return User.objects(ext_id=str(user_identifier)).first()


def user_is_admin(user_identifier: Union[str, ObjectId]) -> bool:
    """
    Verifica si un usuario es administrador.
    """
    user = get_user_by_identifier(user_identifier)
    return bool(user and user.admin)


def get_user_roles(user_identifier: Union[str, ObjectId]) -> List[Dict]:
    """
    Obtiene los roles serializados.
    """
    user = get_user_by_identifier(user_identifier)

    if not user:
        return []
    return _build_permission_index(user)["roles"]

def user_has_action(
    user_db: Dict,
    actions: Union[str, Actions, List[str], List[Actions]],
) -> bool:
    """
    Verifica si posee alguna de las acciones.
    """

    if not user_db:
        return False

    if user_db.get("admin", False):
        return True

    if not isinstance(actions, (list, tuple, set)):
        actions = [actions]

    actions = {
        action.value if isinstance(action, Actions) else action for action in actions
    }

    user_actions = set(user_db.get("actions", []))

    return bool(user_actions & actions)

def user_has_permission(
    user_db: Dict,
    actions: Union[str, Actions, List[str], List[Actions]],
    option: Union[str, Options],
) -> bool:
    """
    Verifica si posee una opción sobre alguna acción.
    """

    if not user_db:
        return False

    if user_db.get("admin", False):
        return True

    if not isinstance(actions, (list, tuple, set)):
        actions = [actions]

    actions = [
        action.value if isinstance(action, Actions) else action for action in actions
    ]

    option = option.value if isinstance(option, Options) else option

    permissions = {
        permission["action"]: set(permission["options"])
        for permission in user_db.get("permissions", [])
    }

    for action in actions:

        if option in permissions.get(action, set()):
            return True

    return False

def user_has_permissions(
    user_db: Dict,
    permissions: List[Dict],
    require_all: bool = False,
) -> bool:
    """
    Verifica uno o varios permisos.

    permissions = [

        {
            "actions": PermissionGroups.FARM,
            "option": Options.READ
        },

        {
            "actions": PermissionGroups.ENTERPRISE,
            "option": Options.UPDATE
        }

    ]
    """

    if not permissions:
        return True

    results = [
        user_has_permission(
            user_db,
            permission["actions"],
            permission["option"],
        )
        for permission in permissions
    ]

    return all(results) if require_all else any(results)


def get_user_actions(user_identifier: Union[str, ObjectId]) -> List[str]:
    """
    Obtiene todas las acciones.
    """

    user = get_user_by_identifier(user_identifier)

    if not user:
        return []

    index = _build_permission_index(user)

    return sorted(index["actions"])


def get_user_permissions(user_identifier: Union[str, ObjectId]) -> List[Dict]:
    """
    Obtiene todos los permisos agrupados por acción.
    """

    user = get_user_by_identifier(user_identifier)

    if not user:
        return []

    index = _build_permission_index(user)

    return [
        {
            "action": action,
            "options": sorted(list(options)),
        }
        for action, options in index["permissions"].items()
    ]


def serialize_user_permissions(user_identifier: Union[str, ObjectId]) -> Dict:
    """
    Serializa toda la información del usuario para
    guardarla dentro del token validado.
    """

    user = get_user_by_identifier(user_identifier)

    if not user:

        return {
            "id": None,
            "ext_id": None,
            "admin": False,
            "roles": [],
            "actions": [],
            "permissions": [],
        }

    index = _build_permission_index(user)

    return {
        "id": str(user.id),
        "ext_id": user.ext_id,
        "admin": index["admin"],
        "roles": index["roles"],
        "actions": sorted(index["actions"]),
        "permissions": [
            {
                "action": action,
                "options": sorted(list(options)),
            }
            for action, options in index["permissions"].items()
        ],
    }
