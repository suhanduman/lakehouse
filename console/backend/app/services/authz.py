"""OIDC claim -> role mapping + action permission matrix.

Maps the `groups` claim from an OIDC/AD token onto internal `Role`s, and
answers "can this set of roles perform this action" via a static
role -> allowed-actions matrix (`_MATRIX`).

No network/IdP calls here — this is pure claim-dict-in, role/bool-out logic,
so it's unit-testable without a running OIDC provider. The API layer (later
task) is responsible for extracting `claims` from the verified JWT/session
and calling `roles_from_claims` / `can`.
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, Set


class Role(Enum):
    ADMIN = "ADMIN"
    ANALYST = "ANALYST"
    STUDENT = "STUDENT"


class Action(Enum):
    READ = "READ"
    SOURCE_CREATE = "SOURCE_CREATE"
    SOURCE_EDIT = "SOURCE_EDIT"
    SOURCE_DELETE_PIPELINE = "SOURCE_DELETE_PIPELINE"
    SOURCE_DELETE_WITH_DATA = "SOURCE_DELETE_WITH_DATA"
    TABLE_CREATE = "TABLE_CREATE"


# AD/OIDC `groups` claim value -> internal Role.
_GROUP_TO_ROLE: Dict[str, Role] = {
    "lakehouse-admins": Role.ADMIN,
    "lakehouse-analysts": Role.ANALYST,
    "lakehouse-students": Role.STUDENT,
}


# Role -> allowed Actions. ADMIN gets everything; ANALYST gets everything
# except SOURCE_DELETE_WITH_DATA (destructive data-loss delete); STUDENT is
# READ-only.
_MATRIX: Dict[Role, Set[Action]] = {
    Role.ADMIN: {
        Action.READ,
        Action.SOURCE_CREATE,
        Action.SOURCE_EDIT,
        Action.SOURCE_DELETE_PIPELINE,
        Action.SOURCE_DELETE_WITH_DATA,
        Action.TABLE_CREATE,
    },
    Role.ANALYST: {
        Action.READ,
        Action.SOURCE_CREATE,
        Action.SOURCE_EDIT,
        Action.SOURCE_DELETE_PIPELINE,
        Action.TABLE_CREATE,
    },
    Role.STUDENT: {
        Action.READ,
    },
}


def roles_from_claims(claims: dict) -> Set[Role]:
    """Map the `groups` claim (list of AD group names) to internal Roles.

    Unknown/unmapped group names are silently ignored (no role granted). A
    missing `groups` claim yields an empty role set.
    """
    groups = claims.get("groups") or []
    return {_GROUP_TO_ROLE[g] for g in groups if g in _GROUP_TO_ROLE}


def can(roles: Set[Role], action: Action) -> bool:
    """True if any of `roles` grants `action` per `_MATRIX`."""
    return any(action in _MATRIX.get(role, set()) for role in roles)
