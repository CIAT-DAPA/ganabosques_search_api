import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bson import ObjectId

from src.auth.utils import (
    get_user_actions,
    get_user_by_identifier,
    get_user_options,
    get_user_roles,
    serialize_user_permissions,
    user_has_action,
    user_has_option,
    user_has_permissions,
    user_is_admin,
)
from ganabosques_orm.enums.actions import Actions
from ganabosques_orm.enums.options import Options


class TestAuthUtils(unittest.TestCase):

    def _first_action(self):
        return list(Actions)[0]

    def _second_action(self):
        actions = list(Actions)
        return actions[1] if len(actions) > 1 else actions[0]

    def _first_option(self):
        return list(Options)[0]

    def _second_option(self):
        options = list(Options)
        return options[1] if len(options) > 1 else options[0]

    @patch("src.auth.utils.User")
    @patch("src.auth.utils.Role")
    def test_get_user_roles_returns_serialized_roles(self, mock_role, mock_user):
        role_1_ref = SimpleNamespace(id=ObjectId())
        role_2_ref = SimpleNamespace(id=ObjectId())

        user = SimpleNamespace(role=[role_1_ref, role_2_ref])
        mock_user_queryset = MagicMock()
        mock_user_queryset.first.return_value = user
        mock_user.objects.return_value = mock_user_queryset

        role_1 = SimpleNamespace(
            id=role_1_ref.id,
            name="Admin",
            actions=[self._first_action()],
            options=[self._first_option()],
        )
        role_2 = SimpleNamespace(
            id=role_2_ref.id,
            name="Reader",
            actions=[self._second_action()],
            options=[self._second_option()],
        )

        def role_objects_side_effect(**kwargs):
            queryset = MagicMock()
            if kwargs["id"] == role_1_ref.id:
                queryset.first.return_value = role_1
            else:
                queryset.first.return_value = role_2
            return queryset

        mock_role.objects.side_effect = role_objects_side_effect

        result = get_user_roles("ext-123")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "Admin")
        self.assertEqual(result[1]["name"], "Reader")
        self.assertEqual(result[0]["actions"], [self._first_action().value])
        self.assertEqual(result[0]["options"], [self._first_option().value])

    @patch("src.auth.utils.User")
    def test_get_user_roles_returns_empty_list_when_user_does_not_exist(self, mock_user):
        queryset = MagicMock()
        queryset.first.return_value = None
        mock_user.objects.return_value = queryset

        self.assertEqual(get_user_roles("missing-user"), [])

    @patch("src.auth.utils.User")
    def test_get_user_by_identifier_uses_ext_id_when_identifier_is_not_objectid_like(self, mock_user):
        user = SimpleNamespace(ext_id="ext-1")
        queryset = MagicMock()
        queryset.first.return_value = user
        mock_user.objects.return_value = queryset

        result = get_user_by_identifier("ext-1")

        self.assertEqual(result.ext_id, "ext-1")
        mock_user.objects.assert_called_once_with(ext_id="ext-1")

    @patch("src.auth.utils.User")
    def test_get_user_by_identifier_tries_objectid_when_identifier_has_24_chars(self, mock_user):
        object_id_str = str(ObjectId())
        user = SimpleNamespace(ext_id="ext-obj")
        queryset = MagicMock()
        queryset.first.return_value = user
        mock_user.objects.return_value = queryset

        result = get_user_by_identifier(object_id_str)

        self.assertEqual(result.ext_id, "ext-obj")

    @patch("src.auth.utils.get_user_by_identifier")
    def test_user_is_admin_returns_true_for_admin_user(self, mock_get_user):
        mock_get_user.return_value = SimpleNamespace(admin=True)

        self.assertTrue(user_is_admin("ext-1"))

    @patch("src.auth.utils.get_user_by_identifier")
    def test_user_is_admin_returns_false_when_user_is_missing(self, mock_get_user):
        mock_get_user.return_value = None

        self.assertFalse(user_is_admin("ext-1"))

    @patch("src.auth.utils.get_user_by_identifier")
    def test_user_has_permissions_returns_true_for_admin(self, mock_get_user):
        mock_get_user.return_value = SimpleNamespace(admin=True)

        result = user_has_permissions(
            "ext-1",
            required_actions=[self._first_action()],
            required_options=[self._first_option()],
        )

        self.assertTrue(result)

    @patch("src.auth.utils.get_user_roles")
    @patch("src.auth.utils.get_user_by_identifier")
    def test_user_has_permissions_returns_false_when_user_does_not_exist(
        self,
        mock_get_user,
        mock_get_roles,
    ):
        mock_get_user.return_value = None
        mock_get_roles.return_value = []

        self.assertFalse(
            user_has_permissions(
                "missing-user",
                required_actions=[self._first_action()],
            )
        )

    @patch("src.auth.utils.get_user_roles")
    @patch("src.auth.utils.get_user_by_identifier")
    def test_user_has_permissions_requires_actions_and_options_in_same_role(
        self,
        mock_get_user,
        mock_get_roles,
    ):
        mock_get_user.return_value = SimpleNamespace(admin=False)
        mock_get_roles.return_value = [
            {
                "name": "Role A",
                "actions": [self._first_action().value],
                "options": [],
            },
            {
                "name": "Role B",
                "actions": [],
                "options": [self._first_option().value],
            },
        ]

        result = user_has_permissions(
            "ext-1",
            required_actions=[self._first_action()],
            required_options=[self._first_option()],
        )

        self.assertFalse(result)

    @patch("src.auth.utils.get_user_roles")
    @patch("src.auth.utils.get_user_by_identifier")
    def test_user_has_permissions_returns_true_when_same_role_contains_actions_and_options(
        self,
        mock_get_user,
        mock_get_roles,
    ):
        mock_get_user.return_value = SimpleNamespace(admin=False)
        mock_get_roles.return_value = [
            {
                "name": "Role A",
                "actions": [self._first_action().value, self._second_action().value],
                "options": [self._first_option().value],
            }
        ]

        result = user_has_permissions(
            "ext-1",
            required_actions=[self._first_action()],
            required_options=[self._first_option()],
        )

        self.assertTrue(result)

    @patch("src.auth.utils.get_user_roles")
    @patch("src.auth.utils.get_user_by_identifier")
    def test_user_has_permissions_validates_actions_only_with_require_all_false(
        self,
        mock_get_user,
        mock_get_roles,
    ):
        mock_get_user.return_value = SimpleNamespace(admin=False)
        mock_get_roles.return_value = [
            {
                "name": "Role A",
                "actions": [self._first_action().value],
                "options": [],
            }
        ]

        result = user_has_permissions(
            "ext-1",
            required_actions=[self._first_action(), self._second_action()],
            require_all_actions=False,
        )

        self.assertTrue(result)

    @patch("src.auth.utils.get_user_roles")
    @patch("src.auth.utils.get_user_by_identifier")
    def test_user_has_permissions_validates_options_only_with_require_all_true(
        self,
        mock_get_user,
        mock_get_roles,
    ):
        mock_get_user.return_value = SimpleNamespace(admin=False)
        mock_get_roles.return_value = [
            {
                "name": "Role A",
                "actions": [],
                "options": [self._first_option().value],
            }
        ]

        result = user_has_permissions(
            "ext-1",
            required_options=[self._first_option(), self._second_option()],
            require_all_options=True,
        )

        self.assertFalse(result)

    @patch("src.auth.utils.user_has_permissions")
    def test_user_has_action_delegates_to_user_has_permissions(self, mock_has_permissions):
        mock_has_permissions.return_value = True

        result = user_has_action("ext-1", self._first_action())

        self.assertTrue(result)
        mock_has_permissions.assert_called_once_with(
            "ext-1",
            required_actions=[self._first_action()],
        )

    @patch("src.auth.utils.user_has_permissions")
    def test_user_has_option_delegates_to_user_has_permissions(self, mock_has_permissions):
        mock_has_permissions.return_value = True

        result = user_has_option("ext-1", self._first_option())

        self.assertTrue(result)
        mock_has_permissions.assert_called_once_with(
            "ext-1",
            required_options=[self._first_option()],
        )

    @patch("src.auth.utils.get_user_roles")
    def test_get_user_actions_returns_unique_actions(self, mock_get_roles):
        mock_get_roles.return_value = [
            {
                "actions": [self._first_action().value, self._second_action().value],
                "options": [],
            },
            {
                "actions": [self._first_action().value],
                "options": [],
            },
        ]

        result = get_user_actions("ext-1")

        self.assertEqual(set(result), {self._first_action().value, self._second_action().value})

    @patch("src.auth.utils.get_user_roles")
    def test_get_user_options_returns_unique_options(self, mock_get_roles):
        mock_get_roles.return_value = [
            {
                "actions": [],
                "options": [self._first_option().value, self._second_option().value],
            },
            {
                "actions": [],
                "options": [self._first_option().value],
            },
        ]

        result = get_user_options("ext-1")

        self.assertEqual(set(result), {self._first_option().value, self._second_option().value})

    @patch("src.auth.utils.get_user_by_identifier")
    def test_serialize_user_permissions_returns_empty_structure_when_user_does_not_exist(
        self,
        mock_get_user,
    ):
        mock_get_user.return_value = None

        result = serialize_user_permissions("missing-user")

        self.assertEqual(
            result,
            {
                "id": None,
                "ext_id": None,
                "admin": False,
                "roles": [],
                "all_actions": [],
                "all_options": [],
            },
        )

    @patch("src.auth.utils.get_user_options")
    @patch("src.auth.utils.get_user_actions")
    @patch("src.auth.utils.get_user_roles")
    @patch("src.auth.utils.get_user_by_identifier")
    def test_serialize_user_permissions_returns_full_structure(
        self,
        mock_get_user,
        mock_get_roles,
        mock_get_actions,
        mock_get_options,
    ):
        user = SimpleNamespace(id=ObjectId(), ext_id="ext-1", admin=True)
        mock_get_user.return_value = user
        mock_get_roles.return_value = [{"name": "Admin", "actions": [], "options": []}]
        mock_get_actions.return_value = ["API_FARMS"]
        mock_get_options.return_value = ["READ"]

        result = serialize_user_permissions("ext-1")

        self.assertEqual(result["ext_id"], "ext-1")
        self.assertTrue(result["admin"])
        self.assertEqual(result["roles"], [{"name": "Admin", "actions": [], "options": []}])
        self.assertEqual(result["all_actions"], ["API_FARMS"])
        self.assertEqual(result["all_options"], ["READ"])


if __name__ == "__main__":
    unittest.main()