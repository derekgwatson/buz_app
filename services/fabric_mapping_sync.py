from services.check_fabric_group_mappings import check_inventory_groups_against_unleashed
from services.database import DatabaseManager
import logging

logger = logging.getLogger(__name__)


def sync_fabric_mappings(db_manager: DatabaseManager, config_path="config.json", output_dir="uploads"):
    """
    Run fabric mapping validation, update local DB, and generate Buz upload files.
    """
    report = check_inventory_groups_against_unleashed(db_manager)
    logger.info(f"Found {len(report)} fabric mapping issues to fix")

    from .fabric_upload_generator import update_fabric_mappings_from_report
    return update_fabric_mappings_from_report(db_manager, report, config_path, output_dir)
