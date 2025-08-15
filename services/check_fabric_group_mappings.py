import logging
from flask import current_app, g
from services.database import DatabaseManager, DatabaseError
from datetime import datetime

logger = logging.getLogger(__name__)


def _norm_code(s):
    if s is None:
        return None
    s = str(s).strip()
    # your rule: remove leading asterisks
    s = s.lstrip("*")
    return s.upper()


def _norm_group_name(s):
    # make ProductGroup comparison robust to spacing/case
    return str(s).strip()


def check_inventory_groups_against_unleashed(db_manager: DatabaseManager):
    """
    Validate fabric group mappings by comparing Unleashed products
    against local fabrics and fabric_group_mappings.

    Returns:
        list of dict: Each dict contains:
            - product_code (str)
            - product_description (str)
            - status (str): 'missing_fabric', 'missing_mapping', or 'invalid_mapping'
            - missing_groups (list[str], optional)
            - invalid_groups (list[dict], optional) with {'group', 'reason'}
    """
    logger.info("🔍 Starting fabric validation (Unleashed → fabrics → fabric_group_mappings)...")

    # Load config
    group_rules_raw = current_app.config.get("unleashed_group_to_inventory_groups", {})
    # Normalize keys once
    group_rules = {_norm_group_name(k): v for k, v in group_rules_raw.items()}

    material_rules = current_app.config.get("material_restrictions_by_group", {})
    supplier_restrictions_raw = current_app.config.get("restricted_supplier_groups", {})
    # normalize supplier keys to UPPER to match _norm_code
    supplier_restrictions = {_norm_code(k): v for k, v in supplier_restrictions_raw.items()}

    report = []

    try:
        # 1. Get Unleashed fabrics
        unleashed_rows = db_manager.execute_query(
            """
            SELECT ProductCode, ProductGroup, ProductDescription, FriendlyDescription2, SupplierCode
            FROM unleashed_products
            WHERE ProductSubGroup IS NOT NULL
              AND TRIM(ProductSubGroup) != ''
              AND UPPER(TRIM(ProductSubGroup)) != 'IGNORE'
            """
        ).fetchall()

        # 2. Build local lookup for fabrics and mappings
        fabrics_lookup = {
            f["supplier_product_code"]: f for f in db_manager.execute_query("SELECT * FROM fabrics").fetchall()
        }
        mappings_lookup = {}
        for row in db_manager.execute_query("SELECT * FROM fabric_group_mappings").fetchall():
            mappings_lookup.setdefault(row["fabric_id"], []).append(row["inventory_group_code"])

        # Optional: quick stats to logs
        logger.info(f"Validator: {len(unleashed_rows)} unleashed rows, {len(fabrics_lookup)} local fabrics")

        seen_groups = set()

        # 3. Process each Unleashed fabric
        for row in unleashed_rows:
            product_code = row["ProductCode"]
            product_group = row["ProductGroup"]
            product_description = row["ProductDescription"]
            material_type = row["FriendlyDescription2"]
            supplier_code = str(row["SupplierCode"]).strip()

            seen_groups.add(product_group)

            # Skip if product group not in rules (log once per new group)
            allowed_groups = group_rules.get(product_group)
            if not allowed_groups:
                continue

            # Supplier restrictions
            restricted_groups = supplier_restrictions.get(supplier_code, [])
            filtered_allowed = [g for g in allowed_groups if g not in restricted_groups]

            # Material restrictions
            final_allowed = []
            for g in filtered_allowed:
                allowed_materials = material_rules.get(g)
                if not allowed_materials or not material_type or material_type in allowed_materials:
                    final_allowed.append(g)

            # Missing fabric?
            fabric_row = fabrics_lookup.get(product_code)
            if not fabric_row:
                report.append({
                    "product_code": product_code,
                    "product_description": product_description,
                    "status": "missing_fabric"
                })
                continue

            # Mapping checks
            fabric_id = fabric_row["id"]
            actual_groups = mappings_lookup.get(fabric_id, [])
            missing_groups = sorted(set(g for g in final_allowed if g not in actual_groups))
            if missing_groups:
                report.append({
                    "product_code": product_code,
                    "product_description": product_description,
                    "status": "missing_mapping",
                    "missing_groups": missing_groups
                })

            invalid_groups = []
            for g in actual_groups:
                if g not in allowed_groups:
                    invalid_groups.append({"group": g, "reason": "not allowed for this ProductGroup"})
                elif g in material_rules:
                    allowed_materials = material_rules[g]
                    if material_type not in allowed_materials:
                        invalid_groups.append({
                            "group": g,
                            "reason": f"material '{material_type}' not allowed (only {allowed_materials})"
                        })
            if invalid_groups:
                report.append({
                    "product_code": product_code,
                    "product_description": product_description,
                    "status": "invalid_mapping",
                    "invalid_groups": invalid_groups
                })

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        report.append({
            "status": "error",
            "message": str(e)
        })

    logger.info(f"Validator finished. Groups seen in Unleashed (sample): {sorted(list(seen_groups))[:12]}")
    logger.info(f"Report items: {len(report)}")
    return report


def update_fabric_mappings_from_report(db_manager: DatabaseManager, report: list):
    """
    Apply changes from a fabric mapping validation report.
    - Adds missing fabrics to `fabrics` (with placeholder fields).
    - Adds missing mappings to `fabric_group_mappings`.
    - Removes invalid mappings from `fabric_group_mappings`.

    Args:
        db_manager: DatabaseManager instance
        report: list of dicts from check_inventory_groups_against_unleashed()
    """
    logger.info("🛠 Applying fabric mapping updates from report...")

    for item in report:
        status = item.get("status")

        # --- Add missing fabrics ---
        if status == "missing_fabric":
            logger.info(f"➕ Adding new fabric {item['product_code']}")
            # TODO: set real supplier_id and descriptions if available
            db_manager.insert_item("fabrics", {
                "supplier_product_code": item["product_code"],
                "supplier_id": None,  # You may want to set this based on rules
                "description_1": item["product_description"],
                "description_2": None,
                "description_3": None,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            })

        # --- Add missing mappings ---
        elif status == "missing_mapping":
            # Lookup fabric ID
            fabric_row = db_manager.execute_query(
                "SELECT id FROM fabrics WHERE supplier_product_code = ?",
                (item["product_code"],)
            ).fetchone()
            if not fabric_row:
                logger.warning(f"⚠️ Cannot add mappings for {item['product_code']} — fabric not found.")
                continue
            fabric_id = fabric_row["id"]
            for group in item["missing_groups"]:
                logger.info(f"➕ Adding mapping: {item['product_code']} → {group}")
                db_manager.insert_item("fabric_group_mappings", {
                    "fabric_id": fabric_id,
                    "inventory_group_code": group
                })

        # --- Remove invalid mappings ---
        elif status == "invalid_mapping":
            fabric_row = db_manager.execute_query(
                "SELECT id FROM fabrics WHERE supplier_product_code = ?",
                (item["product_code"],)
            ).fetchone()
            if not fabric_row:
                logger.warning(f"⚠️ Cannot remove mappings for {item['product_code']} — fabric not found.")
                continue
            fabric_id = fabric_row["id"]
            for bad in item["invalid_groups"]:
                group = bad["group"]
                logger.info(f"🗑 Removing invalid mapping: {item['product_code']} → {group}")
                db_manager.execute_query(
                    "DELETE FROM fabric_group_mappings WHERE fabric_id = ? AND inventory_group_code = ?",
                    (fabric_id, group)
                )

    logger.info("✅ Fabric mapping updates complete.")
