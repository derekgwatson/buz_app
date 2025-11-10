# services/zendesk_service.py
from __future__ import annotations

import os
import re
from typing import Dict, Optional
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
            ...
            Notes Add phone number 0418488548
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
            'discount_group': r'Discount\s+group[:\s]+(.+)',
            'notes': r'Notes[:\s]+(.+)',
        }

        for field, pattern in patterns.items():
            match = re.search(pattern, description, re.IGNORECASE | re.MULTILINE)
            if match:
                fields[field] = match.group(1).strip()

        # Extract phone from notes if present
        phone = None
        if 'notes' in fields:
            # Look for phone numbers in notes (various formats)
            phone_match = re.search(r'(?:phone|mobile|number)[:\s]*(\d[\d\s\-\(\)]+\d)', fields['notes'], re.IGNORECASE)
            if phone_match:
                phone = phone_match.group(1).strip()

        # Get email from ticket subject (format: "Customer onboard: email@example.com")
        email = None
        subject = ticket.subject or ""
        email_match = re.search(r'Customer onboard:\s*(\S+@\S+)', subject, re.IGNORECASE)
        if email_match:
            email = email_match.group(1).strip()

        # Fallback to requester email if not in subject
        if not email and ticket.requester:
            email = ticket.requester.email

        # Validate required fields
        required = ['first_name', 'last_name', 'company_name', 'address']
        missing = [f for f in required if f not in fields]
        if missing:
            raise ValueError(f"Missing required fields in ticket: {', '.join(missing)}")

        if not email:
            raise ValueError("Email not found in ticket subject or requester")

        return CustomerData(
            first_name=fields['first_name'],
            last_name=fields['last_name'],
            company_name=fields['company_name'],
            address=fields['address'],
            email=email,
            phone=phone,
            discount_group=fields.get('discount_group'),
            notes=fields.get('notes')
        )

    def get_customer_data(self, ticket_id: int) -> CustomerData:
        """Fetch ticket and parse customer data in one step"""
        ticket = self.get_ticket(ticket_id)
        return self.parse_customer_data(ticket)
