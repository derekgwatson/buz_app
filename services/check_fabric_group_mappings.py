import logging
from flask import current_app, g
from services.database import DatabaseManager, DatabaseError

logger = logging.getLogger(__name__)


def check_inventory_groups_against_unleashed(db_manager: DatabaseManager):
    logger.info("🔍 Starting fabric validation...")

    group_rules = current_app.config.get("unleashed_group_to_inventory_groups", {})
    material_rules = current_app.config.get("material_restrictions_by_group", {})
    supplier_restrictions = current_app.config.get("restricted_supplier_groups", {})

    violations = []

    try:
        unleashed_rows = db_manager.execute_query(
            """
            SELECT ProductCode, ProductGroup, ProductDescription, FriendlyDescription2, SupplierCode
            FROM unleashed_products
            WHERE ProductSubGroup IS NOT NULL
              AND TRIM(ProductSubGroup) != ''
              AND UPPER(TRIM(ProductSubGroup)) != 'IGNORE'
            """

        ).fetchall()

        for row in unleashed_rows:
            product_code = row["ProductCode"]
            product_group = row["ProductGroup"]
            material_type = row["FriendlyDescription2"]
            product_description = row["ProductDescription"]

            if product_group not in group_rules:
                continue  # skip items with no config rule
            allowed_groups = group_rules[product_group]

            # Filter allowed groups based on material restrictions
            filtered_allowed_groups = []
            raw_supplier_code = row["SupplierCode"]
            supplier_code = str(raw_supplier_code).strip()
            normalized_supplier_code = supplier_code.title()  # Adjust casing as needed
            restricted_groups = supplier_restrictions.get(normalized_supplier_code, [])

            logger.debug(f"🧪 Supplier '{supplier_code}' normalized as '{normalized_supplier_code}'")
            logger.debug(f"🧪 Restricted groups for supplier: {restricted_groups}")

            for group in allowed_groups:
                if group in restricted_groups:
                    continue  # skip restricted groups for this supplier

                allowed_materials = material_rules.get(group)
                if not allowed_materials or not material_type or material_type in allowed_materials:
                    filtered_allowed_groups.append(group)
            logger.debug(
                f"✅ Final allowed groups for {product_code} from {normalized_supplier_code}: {filtered_allowed_groups}"
            )

            inventory_items = db_manager.execute_query(
                "SELECT inventory_group_code FROM inventory_items WHERE SupplierProductCode = ?",
                (product_code,)
            ).fetchall()

            actual_groups = [item["inventory_group_code"] for item in inventory_items]

            if not inventory_items:
                msg = f"❌ Fabric {product_code} - {product_description}  (Group: {product_group}) not found in inventory_items."
                #logger.warning(msg)
                violations.append(msg)
                continue

            # Check for supplier-restricted group usage
            if normalized_supplier_code:
                disallowed_groups = restricted_groups
                if disallowed_groups:
                    bad_groups = [g for g in actual_groups if g in disallowed_groups]
                    if bad_groups:
                        msg = (
                            f"🚫 Fabric {product_code} - {product_description} (Supplier: {supplier_code}) "
                            f"is used in disallowed group(s): {bad_groups}."
                        )
                        #logger.warning(msg)
                        violations.append(msg)
                        continue  # skip further checks for this fabric

            invalid_groups = []
            for group in actual_groups:
                if group not in allowed_groups:
                    invalid_groups.append((group, "not allowed for this ProductGroup"))
                elif group in material_rules:
                    allowed_materials = material_rules[group]
                    if material_type not in allowed_materials:
                        reason = f"material '{material_type}' not allowed (only {allowed_materials})"
                        invalid_groups.append((group, reason))

            # Keep original groups used for messaging
            actual_groups = sorted(set(actual_groups))

            # Now compute missing groups — these are filtered_allowed_groups not found in actual_groups
            missing_groups = sorted(set(g for g in filtered_allowed_groups if g not in actual_groups))

            if invalid_groups or missing_groups:
                invalid_str = (
                    "has invalid group(s): " +
                    ", ".join([f"{grp} ({reason})" for grp, reason in invalid_groups])
                    if invalid_groups else ""
                )
                missing_str = (
                    "is missing required group(s): " + str(missing_groups)
                    if missing_groups else ""
                )

                msg = (
                    f"⚠️ Fabric {product_code} - {product_description} (ProductGroup: {product_group}) "
                    f"{invalid_str}"
                    f"{' and ' if invalid_str and missing_str else ''}"
                    f"{missing_str}. "
                    f"Used in: {actual_groups}, Allowed: {allowed_groups}"
                )
                #logger.warning(msg)
                violations.append(msg)

            for group in actual_groups:
                if group in material_rules:
                    allowed_materials = material_rules[group]
                    if material_type not in allowed_materials:
                        msg = (
                            f"❌ Fabric {product_code} - {product_description} (Material: {material_type}) is used in group '{group}', "
                            f"which only allows {allowed_materials}."
                        )
                        #logger.warning(msg)
                        violations.append(msg)

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        violations.append(f"❌ Database error: {e}")

    logger.info("✅ Fabric validation complete.")
    return violations
