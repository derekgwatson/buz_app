# services/user_management_comparison.py
from __future__ import annotations

import logging
from typing import Dict, Any, List, Set, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class UserRow:
    """
    A row in the user comparison table representing one unique user (by email).
    Shows which orgs they're in and their details.
    """
    email: str
    full_name: str  # Most common full name across orgs
    orgs: Dict[str, Dict[str, Any]]  # org_name -> {group, mfa_enabled, is_active, last_session, user_type}

    def to_dict(self) -> Dict[str, Any]:
        return {
            'email': self.email,
            'full_name': self.full_name,
            'orgs': self.orgs
        }


class UserComparisonTable:
    """
    Comparison table for users across orgs.
    """

    def __init__(self):
        self.org_names: List[str] = []
        self.users: List[UserRow] = []
        self.summary: Dict[str, Any] = {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            'org_names': self.org_names,
            'users': [user.to_dict() for user in self.users],
            'summary': self.summary
        }


def build_user_comparison(result) -> UserComparisonTable:
    """
    Build a user comparison table from the scraping result.

    Args:
        result: UserManagementResult object

    Returns:
        UserComparisonTable with all users organized by email
    """
    table = UserComparisonTable()

    # Get org names
    table.org_names = [org.org_name for org in result.orgs]

    # Build a dictionary of all users by email
    users_by_email: Dict[str, UserRow] = {}

    for org in result.orgs:
        for user in org.users:
            email = user.email.lower()  # Normalize email to lowercase

            if email not in users_by_email:
                # Create new user row
                users_by_email[email] = UserRow(
                    email=user.email,  # Use original casing
                    full_name=user.full_name,
                    orgs={}
                )

            # Add this org's data for this user
            users_by_email[email].orgs[org.org_name] = {
                'group': user.group,
                'mfa_enabled': user.mfa_enabled,
                'is_active': user.is_active,
                'last_session': user.last_session,
                'user_type': user.user_type,
                'full_name': user.full_name  # Store org-specific full name
            }

    # Sort users by email
    table.users = sorted(users_by_email.values(), key=lambda u: u.email.lower())

    # Build summary statistics
    total_unique_users = len(table.users)
    total_user_records = sum(len(org.users) for org in result.orgs)

    # Count users by number of orgs they're in
    users_in_multiple_orgs = len([u for u in table.users if len(u.orgs) > 1])
    users_in_one_org = total_unique_users - users_in_multiple_orgs

    # Count active vs inactive
    active_users = set()
    inactive_users = set()
    for user in table.users:
        has_active = any(org_data['is_active'] for org_data in user.orgs.values())
        has_inactive = any(not org_data['is_active'] for org_data in user.orgs.values())

        if has_active:
            active_users.add(user.email.lower())
        if has_inactive:
            inactive_users.add(user.email.lower())

    table.summary = {
        'total_unique_users': total_unique_users,
        'total_user_records': total_user_records,
        'users_in_multiple_orgs': users_in_multiple_orgs,
        'users_in_one_org': users_in_one_org,
        'users_with_active_access': len(active_users),
        'users_with_inactive_access': len(inactive_users)
    }

    return table
