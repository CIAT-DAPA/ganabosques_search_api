import unittest

from ganabosques_orm.enums.actions import Actions
from ganabosques_orm.enums.options import Options

from src.dependencies.permissions_groups import PermissionGroups
from src.auth.utils import (
    user_has_action,
    user_has_permission,
    user_has_permissions,
)

class TestPermissions(unittest.TestCase):

    def setUp(self):
        self.user = {
            "id": "123",
            "ext_id": "user-1",
            "admin": False,
            "actions": [
                "api_farms",
                "front_farms",
                "front_report",
                "api_enterprise",
            ],
            "permissions": [
                {
                    "action": "api_farms",
                    "options": ["read", "create"],
                },
                {
                    "action": "front_farms",
                    "options": ["read", "update"],
                },
                {
                    "action": "front_report",
                    "options": ["read"],
                },
                {
                    "action": "api_enterprise",
                    "options": ["read", "update"],
                },
            ],
        }

        self.admin = {
            "id": "1",
            "ext_id": "admin",
            "admin": True,
            "actions": [],
            "permissions": [],
        }

    def test_user_has_action_enum(self):
        self.assertTrue(
            user_has_action(self.user, Actions.API_FARMS)
        )

    def test_user_has_action_string(self):
        self.assertTrue(
            user_has_action(self.user, "api_farms")
        )

    def test_user_has_action_group(self):
        self.assertTrue(
            user_has_action(
                self.user,
                PermissionGroups.FARM,
            )
        )

    def test_user_has_action_false(self):
        self.assertFalse(
            user_has_action(
                self.user,
                Actions.API_ADM,
            )
        )

    def test_user_has_action_admin(self):
        self.assertTrue(
            user_has_action(
                self.admin,
                Actions.API_ADM,
            )
        )

    def test_user_has_permission_enum(self):
        self.assertTrue(
            user_has_permission(
                self.user,
                Actions.API_FARMS,
                Options.READ,
            )
        )

    def test_user_has_permission_string(self):
        self.assertTrue(
            user_has_permission(
                self.user,
                "api_farms",
                "read",
            )
        )

    def test_user_has_permission_group(self):
        self.assertTrue(
            user_has_permission(
                self.user,
                PermissionGroups.FARM,
                Options.READ,
            )
        )

    def test_user_has_permission_report_group(self):
        self.assertTrue(
            user_has_permission(
                self.user,
                PermissionGroups.ENTERPRISE,
                Options.READ,
            )
        )

    def test_user_has_permission_invalid_option(self):
        self.assertFalse(
            user_has_permission(
                self.user,
                Actions.API_FARMS,
                Options.DELETE,
            )
        )

    def test_user_has_permission_invalid_action(self):
        self.assertFalse(
            user_has_permission(
                self.user,
                Actions.API_ADM,
                Options.READ,
            )
        )

    def test_user_has_permission_admin(self):
        self.assertTrue(
            user_has_permission(
                self.admin,
                Actions.API_ADM,
                Options.DELETE,
            )
        )

    def test_user_has_permissions_any(self):

        permissions = [
            {
                "actions": PermissionGroups.ADM3,
                "option": Options.READ,
            },
            {
                "actions": PermissionGroups.FARM,
                "option": Options.READ,
            },
        ]

        self.assertTrue(
            user_has_permissions(
                self.user,
                permissions,
            )
        )

    def test_user_has_permissions_all(self):

        permissions = [
            {
                "actions": PermissionGroups.FARM,
                "option": Options.READ,
            },
            {
                "actions": PermissionGroups.ENTERPRISE,
                "option": Options.READ,
            },
        ]

        self.assertTrue(
            user_has_permissions(
                self.user,
                permissions,
                require_all=True,
            )
        )

    def test_user_has_permissions_any_false(self):

        permissions = [
            {
                "actions": PermissionGroups.ADM3,
                "option": Options.DELETE,
            },
            {
                "actions": PermissionGroups.ADM3,
                "option": Options.UPDATE,
            },
        ]

        self.assertFalse(
            user_has_permissions(
                self.user,
                permissions,
            )
        )

    def test_user_has_permissions_empty(self):
        self.assertTrue(
            user_has_permissions(
                self.user,
                [],
            )
        )

    def test_user_has_permissions_admin(self):

        permissions = [
            {
                "actions": PermissionGroups.ADM3,
                "option": Options.DELETE,
            }
        ]

        self.assertTrue(
            user_has_permissions(
                self.admin,
                permissions,
            )
        )

    def test_none_user_action(self):
        self.assertFalse(
            user_has_action(
                None,
                Actions.API_FARMS,
            )
        )

    def test_none_user_permission(self):
        self.assertFalse(
            user_has_permission(
                None,
                Actions.API_FARMS,
                Options.READ,
            )
        )

    def test_none_user_permissions(self):

        permissions = [
            {
                "actions": PermissionGroups.FARM,
                "option": Options.READ,
            }
        ]

        self.assertFalse(
            user_has_permissions(
                None,
                permissions,
            )
        )


if __name__ == "__main__":
    unittest.main()