# services/max_discount_comparison.py
from __future__ import annotations

from typing import Dict, List, Optional, Set
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class ProductDiscountRow:
    """A row in the comparison table representing one product across all orgs"""
    # Primary match key
    code: Optional[str]
    description: Optional[str]

    # Discounts by org name
    discounts: Dict[str, Optional[float]]  # org_name -> discount_pct

    # Sequence numbers by org name
    seq_nos: Dict[str, Optional[int]] = None  # org_name -> seq_no

    # Match info
    matched_by_code: bool = True

    def __post_init__(self):
        if self.seq_nos is None:
            self.seq_nos = {}

    def to_dict(self) -> dict:
        return {
            'code': self.code,
            'description': self.description,
            'discounts': self.discounts,
            'seq_nos': self.seq_nos,
            'matched_by_code': self.matched_by_code
        }


class MaxDiscountComparison:
    """Build comparison table of max discounts across orgs"""

    def __init__(self, review_result):
        """
        Initialize comparison from review result.

        Args:
            review_result: MaxDiscountReviewResult from buz_max_discount_review
        """
        self.review_result = review_result
        self.products: List[ProductDiscountRow] = []

    def build_comparison(self):
        """
        Build comparison table by matching products across orgs.

        Matching strategy:
        1. First match by code (column C)
        2. If no code match, match by description (column B)
        """
        logger.info("Building max discount comparison")

        # Build indices for each org
        org_data = {}
        for org_discounts in self.review_result.orgs:
            org_name = org_discounts.org_name

            # Index by code
            by_code = {}
            # Index by description (for fallback matching)
            by_desc = {}

            for ig in org_discounts.inventory_groups:
                if ig.code:
                    by_code[ig.code] = ig
                if ig.description:
                    by_desc[ig.description] = ig

            org_data[org_name] = {
                'by_code': by_code,
                'by_desc': by_desc
            }

        # Collect all unique codes and descriptions across all orgs
        all_codes: Set[str] = set()
        all_descriptions: Set[str] = set()

        for org_discounts in self.review_result.orgs:
            for ig in org_discounts.inventory_groups:
                if ig.code:
                    all_codes.add(ig.code)
                if ig.description:
                    all_descriptions.add(ig.description)

        # Build comparison rows by code first
        processed_codes = set()
        for code in sorted(all_codes):
            if not code:
                continue

            # Get discounts and seq_nos from each org for this code
            discounts = {}
            seq_nos = {}
            description = None

            for org_discounts in self.review_result.orgs:
                org_name = org_discounts.org_name
                org_indices = org_data[org_name]

                if code in org_indices['by_code']:
                    ig = org_indices['by_code'][code]
                    discounts[org_name] = ig.max_discount_pct
                    seq_nos[org_name] = ig.seq_no
                    if not description and ig.description:
                        description = ig.description
                else:
                    discounts[org_name] = None
                    seq_nos[org_name] = None

            self.products.append(ProductDiscountRow(
                code=code,
                description=description,
                discounts=discounts,
                seq_nos=seq_nos,
                matched_by_code=True
            ))
            processed_codes.add(code)

        # Now handle descriptions that weren't matched by code
        for description in sorted(all_descriptions):
            if not description:
                continue

            # Check if this description belongs to a code we already processed
            already_processed = False
            for org_discounts in self.review_result.orgs:
                for ig in org_discounts.inventory_groups:
                    if ig.description == description and ig.code in processed_codes:
                        already_processed = True
                        break
                if already_processed:
                    break

            if already_processed:
                continue

            # Get discounts and seq_nos from each org for this description
            discounts = {}
            seq_nos = {}
            code = None

            for org_discounts in self.review_result.orgs:
                org_name = org_discounts.org_name
                org_indices = org_data[org_name]

                if description in org_indices['by_desc']:
                    ig = org_indices['by_desc'][description]
                    discounts[org_name] = ig.max_discount_pct
                    seq_nos[org_name] = ig.seq_no
                    if not code and ig.code:
                        code = ig.code
                else:
                    discounts[org_name] = None
                    seq_nos[org_name] = None

            # Only add if found in at least one org
            if any(d is not None for d in discounts.values()):
                self.products.append(ProductDiscountRow(
                    code=code,
                    description=description,
                    discounts=discounts,
                    seq_nos=seq_nos,
                    matched_by_code=False
                ))

        logger.info(f"Built comparison with {len(self.products)} products")

        # Sort products by seq_no (use first available seq_no, preferably from Canberra)
        def get_sort_key(product: ProductDiscountRow):
            # Try to get seq_no from Canberra first
            canberra_seq = product.seq_nos.get('Watson Blinds (Canberra)')
            if canberra_seq is not None:
                return canberra_seq

            # Otherwise get first available seq_no
            for seq_no in product.seq_nos.values():
                if seq_no is not None:
                    return seq_no

            # If no seq_no, sort to end
            return float('inf')

        self.products.sort(key=get_sort_key)
        logger.info(f"Sorted products by sequence number")

    def to_dict(self) -> dict:
        """Convert comparison to dictionary for JSON serialization"""
        return {
            'org_names': [org.org_name for org in self.review_result.orgs],
            'products': [p.to_dict() for p in self.products],
            'summary': {
                'total_products': len(self.products),
                'matched_by_code': sum(1 for p in self.products if p.matched_by_code),
                'matched_by_description': sum(1 for p in self.products if not p.matched_by_code)
            }
        }


def build_max_discount_comparison(review_result) -> MaxDiscountComparison:
    """
    Build comparison table from review result.

    Args:
        review_result: MaxDiscountReviewResult from buz_max_discount_review

    Returns:
        MaxDiscountComparison with products matched across orgs
    """
    comparison = MaxDiscountComparison(review_result)
    comparison.build_comparison()
    return comparison
