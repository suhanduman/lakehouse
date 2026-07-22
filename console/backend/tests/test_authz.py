from __future__ import annotations

from app.services.authz import roles_from_claims, can, Role, Action


def test_admin_can_delete_with_data():
    roles = roles_from_claims({"groups": ["lakehouse-admins"]})
    assert Role.ADMIN in roles
    assert can(roles, Action.SOURCE_DELETE_WITH_DATA)


def test_analyst_cannot_delete_with_data_but_can_create():
    roles = roles_from_claims({"groups": ["lakehouse-analysts"]})
    assert can(roles, Action.SOURCE_CREATE)
    assert can(roles, Action.SOURCE_DELETE_PIPELINE)
    assert not can(roles, Action.SOURCE_DELETE_WITH_DATA)


def test_student_read_only():
    roles = roles_from_claims({"groups": ["lakehouse-students"]})
    assert can(roles, Action.READ)
    assert not can(roles, Action.SOURCE_CREATE)


# --------------------------------------------------------------------------
# Additional coverage
# --------------------------------------------------------------------------

def test_admin_can_do_everything():
    roles = roles_from_claims({"groups": ["lakehouse-admins"]})
    for action in Action:
        assert can(roles, action)


def test_unknown_group_yields_no_roles_and_no_permissions():
    roles = roles_from_claims({"groups": ["some-other-group"]})
    assert roles == set()
    for action in Action:
        assert not can(roles, action)


def test_missing_groups_claim_yields_no_roles():
    roles = roles_from_claims({})
    assert roles == set()
    assert not can(roles, Action.READ)


def test_multiple_groups_union_roles():
    roles = roles_from_claims({"groups": ["lakehouse-students", "lakehouse-analysts"]})
    assert Role.STUDENT in roles
    assert Role.ANALYST in roles
    assert can(roles, Action.SOURCE_CREATE)
    assert not can(roles, Action.SOURCE_DELETE_WITH_DATA)


def test_analyst_table_create_allowed():
    roles = roles_from_claims({"groups": ["lakehouse-analysts"]})
    assert can(roles, Action.TABLE_CREATE)


def test_student_cannot_table_create():
    roles = roles_from_claims({"groups": ["lakehouse-students"]})
    assert not can(roles, Action.TABLE_CREATE)
