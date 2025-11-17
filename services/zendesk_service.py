# services/zendesk_service.py
from __future__ import annotations

import os
import re
from typing import Dict, Optional, List
from dataclasses import dataclass
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket


@dataclass
class CustomerData:
    """Parsed customer data from Zendesk ticket"""
    first_name: str
    last_name: str
    company_name: str
    address: str
    email: str
    buz_instances: List[str]  # e.g., ["Watson Blinds", "Designer Drapes"]
    phone: Optional[str] = None
    discount_group: Optional[str] = None
    notes: Optional[str] = None

    @property
    def is_mobile(self) -> bool:
        """Check if phone number is mobile (starts with 04)"""
        if not self.phone:
            return False
        # Strip whitespace and non-digits
        digits = re.sub(r'\D', '', self.phone)
        return digits.startswith('04')


class ZendeskService:
    """Service for fetching and parsing Zendesk tickets"""

    def __init__(self, subdomain: str = None, email: str = None, token: str = None):
        """
        Initialize Zendesk client

        Args:
            subdomain: Zendesk subdomain (e.g., 'watsonblinds')
            email: Zendesk user email
            token: Zendesk API token
        """
        self.subdomain = subdomain or os.getenv('ZENDESK_SUBDOMAIN', 'watsonblinds')
        self.email = email or os.getenv('ZENDESK_EMAIL')
        self.token = token or os.getenv('ZENDESK_API_TOKEN')
        self._client = None

    @property
    def client(self):
        """Lazy initialization of Zenpy client"""
        if self._client is None:
            if not self.email or not self.token:
                raise ValueError(
                    "Zendesk credentials not provided. "
                    "Set ZENDESK_EMAIL and ZENDESK_API_TOKEN environment variables."
                )
            self._client = Zenpy(
                subdomain=self.subdomain,
                email=self.email,
                token=self.token
            )
        return self._client

    def get_ticket(self, ticket_id: int) -> Ticket:
        """Fetch a ticket by ID"""
        return self.client.tickets(id=ticket_id)

    def parse_customer_data(self, ticket: Ticket) -> CustomerData:
        """
        Parse customer data from ticket description

        Expected format:
            First name Terry
            Last Name Hunt
            Company Name Terry Hunt
            Company Address 3 Redgrave Place, Chapman, ACT 2611
            Phone Number 0418488548
            ...
        """
        description = ticket.description or ""

        # Parse fields using regex
        fields = {}

        # Field mappings (case insensitive)
        patterns = {
            'first_name': r'First\s+name[:\s]+(.+)',
            'last_name': r'Last\s+Name[:\s]+(.+)',
            'company_name': r'Company\s+Name[:\s]+(.+)',
            'address': r'Company\s+Address[:\s]+(.+)',
            'phone': r'Phone\s+Number[:\s]+(.+)',
            'buz_instances_raw': r'Which\s+Buz\s+instance\(s\)\?[:\s]+(.+)',
            'discount_group': r'Discount\s+group[:\s]+(.+)',
            'notes': r'Notes[:\s]+(.+)',
        }

        for field, pattern in patterns.items():
            match = re.search(pattern, description, re.IGNORECASE | re.MULTILINE)
            if match:
                fields[field] = match.group(1).strip()

        # Get phone from dedicated field (if present)
        phone = fields.get('phone')

        # Parse Buz instances (comma-separated list)
        buz_instances = []
        if 'buz_instances_raw' in fields:
            # Split by comma and clean up
            raw_instances = fields['buz_instances_raw'].split(',')
            buz_instances = [inst.strip() for inst in raw_instances if inst.strip()]

        # Get email from ticket subject (format: "Customer onboard: email@example.com")
        email = None
        subject = ticket.subject or ""
        email_match = re.search(r'Customer onboard:\s*(\S+@\S+)', subject, re.IGNORECASE)
        if email_match:
            email = email_match.group(1).strip()

        # Validate required fields
        required = ['first_name', 'last_name', 'company_name', 'address']
        missing = [f for f in required if f not in fields]
        if missing:
            raise ValueError(f"Missing required fields in ticket: {', '.join(missing)}")

        if not email:
            raise ValueError(
                f"Email not found in ticket subject. Expected format: 'Customer onboard: email@example.com'. "
                f"Got subject: '{subject}'"
            )

        if not buz_instances:
            raise ValueError(
                "No Buz instances specified in ticket. Expected 'Which Buz instance(s)?: Watson Blinds' or similar."
            )

        return CustomerData(
            first_name=fields['first_name'],
            last_name=fields['last_name'],
            company_name=fields['company_name'],
            address=fields['address'],
            email=email,
            buz_instances=buz_instances,
            phone=phone,
            discount_group=fields.get('discount_group'),
            notes=fields.get('notes')
        )

    def get_customer_data(self, ticket_id: int) -> CustomerData:
        """Fetch ticket and parse customer data in one step"""
        ticket = self.get_ticket(ticket_id)
        return self.parse_customer_data(ticket)
