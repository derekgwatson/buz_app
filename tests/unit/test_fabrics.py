import pytest
from services.fabrics import get_fabric_grid_data


@pytest.fixture
def mock_inventory_group_data_fabrics(get_db_manager):
    """Fixture to populate the inventory_groups table."""
    db_manager = get_db_manager
    db_manager.insert_item("inventory_groups",
                           {
                               "group_code": "GRP1",
                               "group_description": "Inventory Group 1"
                           })
    db_manager.insert_item("inventory_groups",
                           {
                               "group_code": "GRP2",
                               "group_description": "Inventory Group 2"
                           })
    yield db_manager


@pytest.fixture
def mock_fabric_data(get_db_manager):
    """Fixture to insert mock fabric data into the database."""
    db_manager = get_db_manager

    db_manager.insert_item(
        "fabrics", {"supplier_product_code": "FAB001", "description_1": "Sheer", "description_2": "White"})
    db_manager.insert_item(
        "fabrics", {"supplier_product_code": "FAB002", "description_1": "Outdoor", "description_2": "Canvas"})

    yield db_manager


@pytest.fixture
def mock_fabric_group_mapping_data(get_db_manager):
    """Fixture to populate the fabric_group_mappings table."""
    db_manager = get_db_manager
    db_manager.insert_item("fabric_group_mappings", {"fabric_id": 1, "inventory_group_code": "GRP1"})
    db_manager.insert_item("fabric_group_mappings", {"fabric_id": 2, "inventory_group_code": "GRP2"})
    yield db_manager


def test_form_renders(app_context):
    """Test that the fabric creation form renders correctly."""
    response = app_context.test_client().get("/fabrics/create")
    assert response.status_code == 200
    assert b"Code" in response.data
    assert b"Product Type" in response.data
    assert b"Description 1" in response.data
    assert b"Description 2" in response.data
    assert b"Description 3" in response.data


def test_get_fabric_grid_data(mock_fabric_data, mock_inventory_group_data_fabrics, mock_fabric_group_mapping_data):
    # Run the function
    fabric_list, group_list, mapping_set = get_fabric_grid_data(mock_fabric_data)

    # Assertions
    assert len(fabric_list) == 2  # 2 fabrics
    assert len(group_list) == 2  # 2 inventory groups
    assert len(mapping_set) == 2  # 2 mappings

    # Check specific fabric details
    assert fabric_list[1]["description_1"] == "Sheer"
    assert fabric_list[2]["description_1"] == "Outdoor"

    # Check inventory group descriptions
    assert group_list["GRP1"] == "Inventory Group 1"
    assert group_list["GRP2"] == "Inventory Group 2"

    # Check mapping existence
    assert (1, "GRP1") in mapping_set
    assert (2, "GRP2") in mapping_set
