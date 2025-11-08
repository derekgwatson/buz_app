"""
Unit tests for Blinds & Awnings Fabric Sync service.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from decimal import Decimal
import pandas as pd

from services.blinds_awnings_sync import (
    _norm,
    _build_desc_key,
    _q2,
    _is_wholesale_group,
    _normalize_colour_for_desc,
    _build_description,
    _next_code_for_group,
    _check_material_restriction,
    load_groups_config_from_sheet,
    load_fabric_data_from_sheets,
    load_existing_buz_inventory,
    load_existing_buz_pricing,
    compute_changes,
)


class TestUtilityFunctions:
    """Test utility helper functions."""

    def test_norm(self):
        """Test string normalization."""
        assert _norm("  hello  ") == "hello"
        assert _norm(None) == ""
        assert _norm("") == ""
        assert _norm(123) == "123"

    def test_build_desc_key(self):
        """Test description key building."""
        key = _build_desc_key("Brand", "Fabric", "Colour")
        assert key == "brand||fabric||colour"

        # Case insensitive
        key2 = _build_desc_key("BRAND", "FABRIC", "COLOUR")
        assert key2 == key

    def test_q2_conversion(self):
        """Test decimal conversion with 2dp."""
        assert _q2("10.5") == Decimal("10.50")
        assert _q2("10") == Decimal("10.00")
        assert _q2("") == Decimal("0.00")
        assert _q2(None) == Decimal("0.00")
        assert _q2("invalid") == Decimal("0.00")

    def test_is_wholesale_group(self):
        """Test wholesale group detection."""
        assert _is_wholesale_group("WSROLL") is True
        assert _is_wholesale_group("WSZIPS") is True
        assert _is_wholesale_group("ROLL") is False
        assert _is_wholesale_group("AWNGV2") is False

    def test_normalize_colour_for_desc(self):
        """Test colour normalization for descriptions."""
        assert _normalize_colour_for_desc("To Be Confirmed") == "Colour To Be Confirmed"
        assert _normalize_colour_for_desc("to be confirmed") == "Colour To Be Confirmed"
        assert _normalize_colour_for_desc("Red") == "Red"
        assert _normalize_colour_for_desc("") == ""

    def test_build_description(self):
        """Test full description building."""
        desc = _build_description("Roller Blind", "Brand", "Fabric", "Red")
        assert desc == "Roller Blind Brand Fabric Red"

        # With "To Be Confirmed"
        desc2 = _build_description("Roller Blind", "Brand", "Fabric", "To Be Confirmed")
        assert desc2 == "Roller Blind Brand Fabric Colour To Be Confirmed"

        # With empty parts
        desc3 = _build_description("Roller Blind", "Brand", "", "Red")
        assert desc3 == "Roller Blind Brand Red"

    def test_next_code_for_group(self):
        """Test sequential code generation."""
        existing = ["ROLL10000", "ROLL10001", "ROLL10005"]
        next_code = _next_code_for_group(existing, "ROLL")
        assert next_code == "ROLL10006"

        # Empty list
        next_code2 = _next_code_for_group([], "ROLL", start=10000)
        assert next_code2 == "ROLL10000"

        # Mixed codes (ignore non-matching)
        existing3 = ["ROLL10000", "WSROLL123", "ROLL10002"]
        next_code3 = _next_code_for_group(existing3, "ROLL")
        assert next_code3 == "ROLL10003"

    def test_check_material_restriction(self):
        """Test material restriction checking."""
        restrictions = {
            "ZIPSV2": ["Mesh", "PVC"],
            "AWNS2K": ["Canvas", "Acrylic", "Mesh"]
        }

        # Allowed
        assert _check_material_restriction("ZIPSV2", "Mesh Screen", restrictions) is True
        assert _check_material_restriction("ZIPSV2", "PVC Fabric", restrictions) is True
        assert _check_material_restriction("AWNS2K", "Acrylic", restrictions) is True

        # Not allowed
        assert _check_material_restriction("ZIPSV2", "Cotton", restrictions) is False

        # No restriction = allowed
        assert _check_material_restriction("ROLL", "Any Material", restrictions) is True


class TestLoadGroupsConfig:
    """Test loading groups configuration from Google Sheets."""

    def test_load_groups_config_from_sheet(self):
        """Test loading Buz template configuration."""
        mock_service = Mock()

        # Mock Buz template data
        template_data = [
            ["Product", "Code", "Description", "Price Grid Code", "Cost Grid Code", "Discount Group Code", "Category"],
            ["Roller Blind", "ROLL", "Roller Blind", "", "", "RB", "Fabric - Roller Blind"],
            ["WS Roller Blind", "WSROLL", "WS Roller Blind", "WSRB", "WSRB_C", "WSRB", "Fabric - Roller Blind"],
            ["Awning", "AWNGV2", "Awning", "", "", "AWN", "Fabric - Awning"],
        ]

        mock_service.fetch_sheet_data.return_value = template_data

        result = load_groups_config_from_sheet(
            mock_service,
            "sheet_id_123",
            "Buz template"
        )

        # Check that all groups are loaded
        assert "ROLL" in result
        assert "WSROLL" in result
        assert "AWNGV2" in result

        # Check ROLL config (retail group, no grid codes)
        assert result["ROLL"]["description_prefix"] == "Roller Blind"
        assert result["ROLL"]["price_grid_code"] is None
        assert result["ROLL"]["cost_grid_code"] is None
        assert result["ROLL"]["discount_group_code"] == "RB"
        assert result["ROLL"]["category"] == "Fabric - Roller Blind"

        # Check WSROLL config (wholesale group, has grid codes)
        assert result["WSROLL"]["description_prefix"] == "WS Roller Blind"
        assert result["WSROLL"]["price_grid_code"] == "WSRB"
        assert result["WSROLL"]["cost_grid_code"] == "WSRB_C"
        assert result["WSROLL"]["discount_group_code"] == "WSRB"
        assert result["WSROLL"]["category"] == "Fabric - Roller Blind"


class TestLoadFabricData:
    """Test Google Sheets data loading."""

    def test_load_fabric_data_from_sheets(self):
        """Test loading fabric data from Google Sheets."""
        # Mock sheets service
        mock_service = Mock()

        # Mock retail tab data
        retail_data = [
            ["FD1", "FD2", "FD3", "Unleashed Code", "Category", "Price"],
            ["Brand1", "Fabric1", "Red", "UNL001", "Fabric - Roller Blind", "45.50"],
            ["Brand2", "Fabric2", "Blue", "UNL002", "Fabric - Roller Blind", "50.00"],
        ]

        # Mock wholesale tab data
        wholesale_data = [
            ["FD1", "FD2", "FD3", "Unleashed Code", "Category", "Price"],
            ["Brand3", "Fabric3", "Green", "UNL003", "Fabric - Roller Blind", "89-3"],
        ]

        mock_service.fetch_sheet_data.side_effect = [retail_data, wholesale_data]

        groups_config = {
            "ROLL": {
                "category": "Fabric - Roller Blind",
                "description_prefix": "Roller Blind"
            },
            "WSROLL": {
                "category": "Fabric - Roller Blind",
                "description_prefix": "WS Roller Blind"
            }
        }

        material_restrictions = {}

        result, filtered = load_fabric_data_from_sheets(
            mock_service,
            "sheet_id_123",
            "Retail",
            "Wholesale",
            groups_config,
            material_restrictions
        )

        # Check retail group (ROLL)
        assert "ROLL" in result
        assert len(result["ROLL"]) == 2
        assert result["ROLL"].iloc[0]["FD1"] == "Brand1"

        # Check wholesale group (WSROLL)
        assert "WSROLL" in result
        assert len(result["WSROLL"]) == 1

        # Check filtered (should be empty with no restrictions)
        assert isinstance(filtered, dict)
        assert len(filtered.get("ROLL", set())) == 0
        assert result["WSROLL"].iloc[0]["FD1"] == "Brand3"


class TestLoadBuzData:
    """Test loading existing Buz data."""

    def test_load_existing_buz_inventory(self, get_db_manager):
        """Test loading existing inventory from database."""
        db = get_db_manager

        # Insert test data (table already created by init_db in fixture)
        db.execute_query("""
            INSERT INTO inventory_items (
                Code, SupplierProductCode, DescnPart1, DescnPart2, DescnPart3,
                Description, Active, Warning, PriceGridCode, CostGridCode,
                DiscountGroupCode, inventory_group_code, PkId
            ) VALUES
            ('ROLL10000', 'UNL001', 'Brand1', 'Fabric1', 'Red',
             'Roller Blind Brand1 Fabric1 Red', 'TRUE', '', '', '', 'RB', 'ROLL', 'pk1'),
            ('WSROLL10000', 'UNL002', 'Brand2', 'Fabric2', 'Blue',
             'WS Roller Blind Brand2 Fabric2 Blue', 'TRUE', '', 'WSRB', 'WSRB_C', 'WSRB', 'WSROLL', 'pk2')
        """)

        groups_config = {
            "ROLL": {"category": "Fabric - Roller Blind"},
            "WSROLL": {"category": "Fabric - Roller Blind"}
        }

        inv_by_group, existing_codes = load_existing_buz_inventory(db, groups_config)

        assert "ROLL" in inv_by_group
        assert "WSROLL" in inv_by_group
        assert len(inv_by_group["ROLL"]) == 1
        assert len(inv_by_group["WSROLL"]) == 1
        assert "ROLL10000" in existing_codes["ROLL"]
        assert "WSROLL10000" in existing_codes["WSROLL"]

    def test_load_existing_buz_pricing(self, get_db_manager):
        """Test loading existing pricing from database."""
        db = get_db_manager

        # Insert test data (table already created by init_db in fixture)
        db.execute_query("""
            INSERT INTO pricing_data (
                InventoryCode, SellLMWide, CostLMWide, DateFrom
            ) VALUES
            ('ROLL10000', 45.50, 30.00, '01/01/2024'),
            ('ROLL10000', 50.00, 35.00, '01/06/2024')
        """)

        groups_config = {
            "ROLL": {"category": "Fabric - Roller Blind"}
        }

        pricing_map = load_existing_buz_pricing(db, groups_config)

        # Should get the latest pricing
        assert "ROLL10000" in pricing_map
        assert pricing_map["ROLL10000"]["sell_price"] == Decimal("50.00")
        assert pricing_map["ROLL10000"]["cost_price"] == Decimal("35.00")


class TestComputeChanges:
    """Test change computation logic."""

    def test_compute_changes_add_operation(self):
        """Test ADD operation for new fabrics."""
        # Setup mock data
        fabrics_by_group = {
            "ROLL": pd.DataFrame([
                {
                    "_key": "brand1||fabric1||red",
                    "FD1": "Brand1",
                    "FD2": "Fabric1",
                    "FD3": "Red",
                    "Unleashed Code": "UNL001",
                    "Price": "45.50"
                }
            ])
        }

        inv_by_group = {
            "ROLL": pd.DataFrame()  # Empty = no existing inventory
        }

        existing_codes = {"ROLL": set()}

        groups_config = {
            "ROLL": {
                "description_prefix": "Roller Blind",
                "price_grid_code": None,
                "cost_grid_code": None,
                "discount_group_code": "RB"
            }
        }

        pricing_map = {}

        items_changes, pricing_changes, change_log, markup_info = compute_changes(
            fabrics_by_group,
            inv_by_group,
            existing_codes,
            groups_config,
            pricing_map,
            {"ROLL": set()},
            {}
        )

        # Check ADD operation
        assert "ROLL" in items_changes
        assert len(items_changes["ROLL"]) == 1

        item = items_changes["ROLL"][0]
        assert item["Operation"] == "A"
        assert item["Code"] == "ROLL10000"
        assert item["Description"] == "Roller Blind Brand1 Fabric1 Red"
        assert item["DescnPart1 (Material)"] == "Brand1"
        assert item["Supplier Product Code"] == "UNL001"

        # Check pricing (retail group)
        assert "ROLL" in pricing_changes
        assert len(pricing_changes["ROLL"]) == 1

        # Check change log (only item ADD is logged, not pricing)
        assert len(change_log) == 1
        assert change_log[0]["Operation"] == "A"

    def test_compute_changes_edit_operation(self):
        """Test EDIT operation for existing fabrics."""
        fabrics_by_group = {
            "ROLL": pd.DataFrame([
                {
                    "_key": "brand1||fabric1||red",
                    "FD1": "Brand1",
                    "FD2": "Fabric1",
                    "FD3": "Red",
                    "Unleashed Code": "UNL001_NEW",  # Changed code
                    "Price": "45.50"
                }
            ])
        }

        inv_by_group = {
            "ROLL": pd.DataFrame([
                {
                    "_key": "brand1||fabric1||red",
                    "Code": "ROLL10000",
                    "SupplierProductCode": "UNL001_OLD",
                    "DescnPart1": "Brand1",
                    "DescnPart2": "Fabric1",
                    "DescnPart3": "Red",
                    "Active": "TRUE",
                    "Warning": "",
                    "PkId": "pk1",
                    "PriceGridCode": "",
                    "CostGridCode": "",
                    "DiscountGroupCode": "RB"
                }
            ])
        }

        existing_codes = {"ROLL": {"ROLL10000"}}

        groups_config = {
            "ROLL": {
                "description_prefix": "Roller Blind",
                "price_grid_code": None,
                "cost_grid_code": None,
                "discount_group_code": "RB"
            }
        }

        pricing_map = {}

        items_changes, pricing_changes, change_log, markup_info = compute_changes(
            fabrics_by_group,
            inv_by_group,
            existing_codes,
            groups_config,
            pricing_map,
            {"ROLL": set()},
            {}
        )

        # Check EDIT operation
        assert "ROLL" in items_changes
        assert len(items_changes["ROLL"]) == 1

        item = items_changes["ROLL"][0]
        assert item["Operation"] == "E"
        assert item["Code"] == "ROLL10000"
        assert item["Supplier Product Code"] == "UNL001_NEW"

        # Check change log
        assert any("Supplier code changed" in c["Reason"] for c in change_log)

    def test_compute_changes_deprecate_operation(self):
        """Test DEPRECATE operation for missing fabrics."""
        fabrics_by_group = {
            "ROLL": pd.DataFrame()  # No fabrics in sheet
        }

        inv_by_group = {
            "ROLL": pd.DataFrame([
                {
                    "_key": "brand1||fabric1||red",
                    "Code": "ROLL10000",
                    "SupplierProductCode": "UNL001",
                    "DescnPart1": "Brand1",
                    "DescnPart2": "Fabric1",
                    "DescnPart3": "Red",
                    "Active": "TRUE",
                    "Warning": "",
                    "PkId": "pk1",
                    "PriceGridCode": "",
                    "CostGridCode": "",
                    "DiscountGroupCode": "RB"
                }
            ])
        }

        existing_codes = {"ROLL": {"ROLL10000"}}

        groups_config = {
            "ROLL": {
                "description_prefix": "Roller Blind",
                "price_grid_code": None,
                "cost_grid_code": None,
                "discount_group_code": "RB"
            }
        }

        pricing_map = {}

        items_changes, pricing_changes, change_log, markup_info = compute_changes(
            fabrics_by_group,
            inv_by_group,
            existing_codes,
            groups_config,
            pricing_map,
            {"ROLL": set()},
            {}
        )

        # Check DEPRECATE operation
        assert "ROLL" in items_changes
        assert len(items_changes["ROLL"]) == 1

        item = items_changes["ROLL"][0]
        assert item["Operation"] == "E"
        assert item["Warning"] == "Deprecated - DO NOT USE"
        assert item["Active"] == "TRUE"  # Kept active

        # Check change log
        assert any(c["Operation"] == "D" for c in change_log)

    def test_compute_changes_wholesale_no_pricing(self):
        """Test that wholesale groups don't generate pricing changes."""
        fabrics_by_group = {
            "WSROLL": pd.DataFrame([
                {
                    "_key": "brand1||fabric1||red",
                    "FD1": "Brand1",
                    "FD2": "Fabric1",
                    "FD3": "Red",
                    "Unleashed Code": "UNL001",
                    "Price": "89-3"  # Price category, not price
                }
            ])
        }

        inv_by_group = {"WSROLL": pd.DataFrame()}
        existing_codes = {"WSROLL": set()}

        groups_config = {
            "WSROLL": {
                "description_prefix": "WS Roller Blind",
                "price_grid_code": "WSRB",
                "cost_grid_code": "WSRB_C",
                "discount_group_code": "WSRB"
            }
        }

        pricing_map = {}

        items_changes, pricing_changes, change_log, markup_info = compute_changes(
            fabrics_by_group,
            inv_by_group,
            existing_codes,
            groups_config,
            pricing_map,
            {"ROLL": set()},
            {}
        )

        # Check items created with grid codes
        assert "WSROLL" in items_changes
        item = items_changes["WSROLL"][0]
        assert item["Price Grid Code"] == "WSRB"
        assert item["Cost Grid Code"] == "WSRB_C"

        # No pricing changes for wholesale
        assert "WSROLL" not in pricing_changes or len(pricing_changes["WSROLL"]) == 0
