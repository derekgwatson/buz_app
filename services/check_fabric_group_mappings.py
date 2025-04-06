from flask import current_app
import logging
from database import DatabaseManager, DatabaseError

logger = logging.getLogger(__name__)


def check_unmatched_fabrics(db: DatabaseManager):
    query = """
        SELECT up.ProductCode
        FROM unleashed_products up
        LEFT JOIN inventory_items ii ON up.ProductCode = ii.SupplierProductCode
        LEFT JOIN fabrics f ON up.ProductCode = f.supplier_product_code
        WHERE ii.id IS NULL OR f.id IS NULL
    """
    try:
        rows = db.execute_query(query).fetchall()
        for row in rows:
            logger.warning(f"❌ Fabric {row['ProductCode']} is missing from inventory_items or fabrics table.")
    except DatabaseError as e:
        logger.error(f"Database error while checking unmatched fabrics: {e}")


def check_fabric_group_mappings(db: DatabaseManager, rules: dict[str, list[str]]):
    query = """
        SELECT up.ProductCode, up.ProductGroup, ii.inventory_group_code, f.id AS fabric_id
        FROM unleashed_products up
        JOIN inventory_items ii ON up.ProductCode = ii.SupplierProductCode
        JOIN fabrics f ON f.supplier_product_code = up.ProductCode
    """

    try:
        rows = db.execute_query(query).fetchall()

        for row in rows:
            product_code = row['ProductCode']
            product_group = row['ProductGroup']
            allowed_groups = rules.get(product_group, [])
            fabric_id = row['fabric_id']

            mapping_query = "SELECT inventory_group_code FROM fabric_group_mappings WHERE fabric_id = ?"
            mapped_groups = db.execute_query(mapping_query, (fabric_id,)).fetchall()
            mapped_group_codes = [r['inventory_group_code'] for r in mapped_groups]

            if not mapped_group_codes:
                logger.warning(
                    f"❌ Fabric {product_code} (Group: {product_group}) exists but has NO group mappings. "
                    f"Expected one or more of: {allowed_groups}"
                )
                continue

            invalid_groups = [g for g in mapped_group_codes if g not in allowed_groups]
            missing_groups = [g for g in allowed_groups if g not in mapped_group_codes]

            if invalid_groups or missing_groups:
                logger.warning(
                    f"⚠️ Fabric {product_code} (Group: {product_group}) "
                    f"{'has invalid group(s): ' + str(invalid_groups) if invalid_groups else ''}"
                    f"{' and ' if invalid_groups and missing_groups else ''}"
                    f"{'is missing required group(s): ' + str(missing_groups) if missing_groups else ''}. "
                    f"Mapped: {mapped_group_codes}, Allowed: {allowed_groups}"
                )

    except DatabaseError as e:
        logger.error(f"Database error while checking fabric mappings: {e}")


def run_fabric_group_validation():
    from flask import g

    logger.info("🔍 Starting fabric group validation...")

    rules = current_app.config.get("unleashed_group_to_inventory_groups", {})
    if not rules:
        logger.warning("⚠️ No rules found in config — skipping validation.")
        return

    check_unmatched_fabrics(g.db)
    check_fabric_group_mappings(g.db, rules)

    logger.info("✅ Fabric group validation complete.")
