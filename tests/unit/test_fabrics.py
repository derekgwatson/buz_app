from services.fabrics import get_fabric_grid_data


def test_form_renders(client):
    """Test that the fabric creation form renders correctly."""
    response = client.get("/fabrics/create")
    assert response.status_code == 200
    assert b"Code" in response.data
    assert b"Product Type" in response.data
    assert b"Description 1" in response.data
    assert b"Description 2" in response.data
    assert b"Description 3" in response.data


def test_get_fabric_grid_data(mock_fabric_data):
    db_manager = mock_fabric_data

    # Run the function
    fabric_list, group_list, mapping_set = get_fabric_grid_data(db_manager)

    # Assertions
    assert len(fabric_list) == 2  # 2 fabrics
    assert len(group_list) == 2  # 2 inventory groups
    assert len(mapping_set) == 2  # 2 mappings

    # Check specific fabric details
    assert fabric_list[1]["description_1"] == "Sheer"
    assert fabric_list[2]["description_1"] == "Outdoor"

    # Check inventory group descriptions
    assert group_list["GRP1"] == "Blinds"
    assert group_list["GRP2"] == "Awnings"

    # Check mapping existence
    assert (1, "GRP1") in mapping_set
    assert (2, "GRP2") in mapping_set
