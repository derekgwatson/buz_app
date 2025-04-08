import logging
from flask import current_app, g
from services.database import DatabaseManager, DatabaseError

logger = logging.getLogger(__name__)


def check_inventory_groups_against_unleashed(db_manager: DatabaseManager):
    logger.info("🔍 Starting fabric validation...")

    group_rules = current_app.config.get("unleashed_group_to_inventory_groups", {})
    material_rules = current_app.config.get("material_restrictions_by_group", {})

    violations = []

    try:
        unleashed_rows = db_manager.execute_query(
            "SELECT ProductCode, ProductGroup, FriendlyDescription2 FROM unleashed_products"
        ).fetchall()

        for row in unleashed_rows:
            product_code = row["ProductCode"]
            product_group = row["ProductGroup"]
            material_type = row["FriendlyDescription2"]

            if product_group not in group_rules:
                continue  # skip items with no config rule
            allowed_groups = group_rules[product_group]
            # Filter allowed groups based on material restrictions
            filtered_allowed_groups = []
            for group in allowed_groups:
                allowed_materials = material_rules.get(group)
                if not allowed_materials or not material_type or material_type in allowed_materials:
                    filtered_allowed_groups.append(group)

            inventory_items = db_manager.execute_query(
                "SELECT inventory_group_code FROM inventory_items WHERE SupplierProductCode = ?",
                (product_code,)
            ).fetchall()

            actual_groups = [item["inventory_group_code"] for item in inventory_items]

            if not inventory_items:
                msg = f"❌ Fabric {product_code} (Group: {product_group}) not found in inventory_items."
                logger.warning(msg)
                violations.append(msg)
                continue

            invalid_groups = sorted(set(g for g in actual_groups if g not in filtered_allowed_groups))
            missing_groups = sorted(set(g for g in filtered_allowed_groups if g not in actual_groups))
            actual_groups = sorted(set(actual_groups))  # clean up used list too

            if invalid_groups or missing_groups:
                msg = (
                    f"⚠️ Fabric {product_code} (ProductGroup: {product_group}) "
                    f"{'has invalid group(s): ' + str(invalid_groups) if invalid_groups else ''}"
                    f"{' and ' if invalid_groups and missing_groups else ''}"
                    f"{'is missing required group(s): ' + str(missing_groups) if missing_groups else ''}. "
                    f"Used in: {actual_groups}, Allowed: {allowed_groups}"
                )
                logger.warning(msg)
                violations.append(msg)

            for group in actual_groups:
                if group in material_rules:
                    allowed_materials = material_rules[group]
                    if material_type not in allowed_materials:
                        msg = (
                            f"❌ Fabric {product_code} (Material: {material_type}) is used in group '{group}', "
                            f"which only allows {allowed_materials}."
                        )
                        logger.warning(msg)
                        violations.append(msg)

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        violations.append(f"❌ Database error: {e}")

    logger.info("✅ Fabric validation complete.")
    return violations
