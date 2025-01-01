from services.remove_old_items import get_old_buz_items_unleashed, remove_old_items, get_headers_config


def test_remove_old_items(mock_unleashed_data, app_config, get_db_manager):
    db_manager = get_db_manager
    output_file = "test.xlsx"
    remove_old_items(db_manager=db_manager, app_config=app_config, output_file=output_file)


def test_get_old_buz_items_unleashed(get_db_manager):
    db_manager = get_db_manager
    result = get_old_buz_items_unleashed(db_manager=db_manager)