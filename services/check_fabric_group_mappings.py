import logging
from flask import current_app
from services.database import DatabaseManager, DatabaseError
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


def _norm_code(s):
    if s is None:
        return None
    s = str(s).strip()
    s = s.lstrip("*")
    return s.upper()


def _norm_group_name(s):
    # make ProductGroup comparison robust to spacing/case
    return str(s or "").strip()


def check_inventory_groups_against_unleashed(db_manager: DatabaseManager):
    """
    Validate fabric group mappings by comparing Unleashed products
    against local fabrics and fabric_group_mappings.

    Returns: list[dict]
        Each dict contains:
            - product_code (str)
            - product_description (str)
            - status (str): 'missing_fabric' | 'missing_mapping' | 'invalid_mapping'
            - missing_groups (list[str], optional)
            - invalid_groups (list[dict], optional) with {'group', 'reason'}
    """
    logger.info("🔍 Starting fabric validation (Unleashed → fabrics → fabric_group_mappings)...")

    # Load config
    group_rules_raw = current_app.config.get("unleashed_group_to_inventory_groups", {})
    group_rules = {_norm_group_name(k): list(v or []) for k, v in group_rules_raw.items()}

    material_rules = current_app.config.get("material_restrictions_by_group", {})  # {group: [allowed materials]}
    supplier_restrictions_raw = current_app.config.get("restricted_supplier_groups", {})  # {supplier_name: [blocked groups]}
    supplier_restrictions = {(_norm_code(k) or ""): list(v or []) for k, v in supplier_restrictions_raw.items()}

    report = []
    seen_groups = set()

    try:
        # 1) Pull Unleashed products to validate
        unleashed_rows = db_manager.execute_query(
            """
            SELECT ProductCode, ProductGroup, ProductDescription, FriendlyDescription2, SupplierCode
            FROM unleashed_products
            WHERE ProductSubGroup IS NOT NULL
              AND TRIM(ProductSubGroup) != ''
              AND UPPER(TRIM(ProductSubGroup)) != 'IGNORE'
              AND ProductCode IS NOT NULL
              AND TRIM(ProductCode) != ''
            """
        ).fetchall()

        # 2) Preload local fabric ids by supplier_product_code (normalized)
        fabrics_by_code = {}
        for f in db_manager.execute_query("SELECT id, supplier_product_code FROM fabrics").fetchall():
            fabrics_by_code[_norm_code(f["supplier_product_code"])] = f["id"]

        # 3) Preload mappings per fabric_id
        mappings_by_fabric_id = defaultdict(list)
        for m in db_manager.execute_query("SELECT fabric_id, inventory_group_code FROM fabric_group_mappings").fetchall():
            mappings_by_fabric_id[m["fabric_id"]].append(m["inventory_group_code"])

        logger.info(f"Validator: {len(unleashed_rows)} unleashed rows, {len(fabrics_by_code)} local fabrics cached")

        # 4) Validate each Unleashed product
        for row in unleashed_rows:
            product_code = str(row["ProductCode"]).strip()
            norm_code = _norm_code(product_code)
            product_group = _norm_group_name(row["ProductGroup"])
            product_description = str(row["ProductDescription"] or "").strip()
            material_type = str(row["FriendlyDescription2"] or "").strip()  # adjust if your "material" lives elsewhere
            supplier_code_raw = row["SupplierCode"]
            supplier_key = _norm_code(supplier_code_raw) or ""

            seen_groups.add(product_group)

            if product_group not in group_rules:
                # Not governed by your rules -> ignore silently
                continue

            allowed_groups = list(group_rules.get(product_group, []))

            # Supplier-based restrictions (exclude some groups entirely)
            blocked_for_supplier = set(supplier_restrictions.get(supplier_key, []))
            allowed_groups = [g for g in allowed_groups if g not in blocked_for_supplier]

            # Material-based restrictions
            final_allowed = []
            for g in allowed_groups:
                allowed_materials = material_rules.get(g)
                if not allowed_materials:
                    final_allowed.append(g)
                else:
                    # only allow if material_type is explicitly allowed
                    if material_type and material_type in allowed_materials:
                        final_allowed.append(g)

            fabric_id = fabrics_by_code.get(norm_code)

            # Case A: fabric not in local db at all
            if not fabric_id:
                report.append({
                    "product_code": product_code,
                    "product_description": product_description,
                    "status": "missing_fabric",
                    "missing_groups": final_allowed,  # optional hint about where it should map
                })
                continue

            # Case B: fabric exists — check mappings
            actual_groups = mappings_by_fabric_id.get(fabric_id, [])
            # Missing groups: required by rules but not mapped
            missing_groups = sorted(g for g in final_allowed if g not in actual_groups)
            if missing_groups:
                report.append({
                    "product_code": product_code,
                    "product_description": product_description,
                    "status": "missing_mapping",
                    "missing_groups": missing_groups
                })

            # Invalid groups: mapped but not allowed by rules/materials
            invalid_groups = []
            allowed_set = set(final_allowed)
            for g in actual_groups:
                if g not in allowed_set:
                    reason = "not allowed by ProductGroup/material/supplier rules"
                    invalid_groups.append({"group": g, "reason": reason})

            if invalid_groups:
                report.append({
                    "product_code": product_code,
                    "product_description": product_description,
                    "status": "invalid_mapping",
                    "invalid_groups": invalid_groups
                })

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        report.append({"status": "error", "message": str(e)})

    logger.info(f"Validator finished. Groups seen in Unleashed (sample): {sorted(list(seen_groups))[:12]}")
    logger.info(f"Report items: {len(report)}")
    return report


def update_fabric_mappings_from_report(db_manager: DatabaseManager, report: list):
    """
    Apply changes from a fabric mapping validation report.
    - Adds missing fabrics to `fabrics` (with placeholder fields).
    - Adds missing mappings to `fabric_group_mappings`.
    - Removes invalid mappings from `fabric_group_mappings`.

    Returns: dict summary counts
    """
    logger.info("🛠 Applying fabric mapping updates from report...")

    added_fabrics = 0
    added_mappings = 0
    removed_mappings = 0

    for item in report:
        status = item.get("status")

        if status == "missing_fabric":
            logger.info(f"➕ Adding new fabric {item['product_code']}")
            now = datetime.utcnow()
            db_manager.insert_item("fabrics", {
                "supplier_product_code": item["product_code"],
                "supplier_id": None,  # set properly if you know the supplier here
                "description_1": item.get("product_description"),
                "description_2": None,
                "description_3": None,
                "created_at": now,
                "updated_at": now
            })
            added_fabrics += 1

        elif status == "missing_mapping":
            fabric_row = db_manager.execute_query(
                "SELECT id FROM fabrics WHERE supplier_product_code = ?",
                (item["product_code"],)
            ).fetchone()
            if not fabric_row:
                logger.warning(f"⚠️ Cannot add mappings for {item['product_code']} — fabric not found.")
                continue

            fabric_id = fabric_row["id"]
            for group in item.get("missing_groups", []):
                logger.info(f"➕ Adding mapping: {item['product_code']} → {group}")
                db_manager.insert_item("fabric_group_mappings", {
                    "fabric_id": fabric_id,
                    "inventory_group_code": group
                })
                added_mappings += 1

        elif status == "invalid_mapping":
            fabric_row = db_manager.execute_query(
                "SELECT id FROM fabrics WHERE supplier_product_code = ?",
                (item["product_code"],)
            ).fetchone()
            if not fabric_row:
                logger.warning(f"⚠️ Cannot remove mappings for {item['product_code']} — fabric not found.")
                continue

            fabric_id = fabric_row["id"]
            for bad in item.get("invalid_groups", []):
                group = bad["group"]
                logger.info(f"🗑 Removing invalid mapping: {item['product_code']} → {group}")
                db_manager.execute_query(
                    "DELETE FROM fabric_group_mappings WHERE fabric_id = ? AND inventory_group_code = ?",
                    (fabric_id, group)
                )
                removed_mappings += 1

    summary = {
        "added_fabrics": added_fabrics,
        "added_mappings": added_mappings,
        "removed_mappings": removed_mappings,
    }
    logger.info(f"✅ Fabric validation changes applied: {summary}")
    return summary
