import logging
from flask import current_app, g
from database import DatabaseManager, DatabaseError

logger = logging.getLogger(__name__)


def check_inventory_groups_against_unleashed():
    """
    Checks if each fabric in `unleashed_products` is correctly used in inventory items
    based on the allowed inventory groups defined in config.
    """
    logger.info("🔍 Starting fabric group validation (simplified)...")

    rules = current_app.config.get("unleashed_group_to_inventory_groups", {})
    if not rules:
        logger.warning("⚠️ No rules found in config — skipping validation.")
        return

    db: DatabaseManager = g.db

    try:
        unleashed_rows = db.execute_query(
            "SELECT ProductCode, ProductGroup FROM unleashed_products"
        ).fetchall()

        for row in unleashed_rows:
            product_code = row["ProductCode"]
            product_group = row["ProductGroup"]
            allowed_groups = rules.get(product_group, [])

            if not allowed_groups:
                logger.warning(
                    f"⚠️ No group rules defined for ProductGroup '{product_group}' (ProductCode: {product_code})"
                )
                continue

            inventory_items = db.execute_query(
                "SELECT inventory_group_code FROM inventory_items WHERE SupplierProductCode = ?",
                (product_code,)
            ).fetchall()

            if not inventory_items:
                logger.warning(
                    f"❌ Fabric {product_code} (Group: {product_group}) not found in inventory_items table."
                )
                continue

            actual_groups = [item["inventory_group_code"] for item in inventory_items]

            invalid_groups = [g for g in actual_groups if g not in allowed_groups]
            missing_groups = [g for g in allowed_groups if g not in actual_groups]

            if invalid_groups or missing_groups:
                logger.warning(
                    f"⚠️ Fabric {product_code} (Group: {product_group}) "
                    f"{'has invalid group(s): ' + str(invalid_groups) if invalid_groups else ''}"
                    f"{' and ' if invalid_groups and missing_groups else ''}"
                    f"{'is missing required group(s): ' + str(missing_groups) if missing_groups else ''}. "
                    f"Used in: {actual_groups}, Allowed: {allowed_groups}"
                )

    except DatabaseError as e:
        logger.error(f"Database error while checking inventory groups: {e}")

    logger.info("✅ Fabric group validation complete.")
