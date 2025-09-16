# services/curtain_sync_db.py
import logging
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

CURTAIN_TABS = ("CRTWT", "CRTNT")
NEW_FABRIC_EFF_DATE_STR = "01/01/2020"

# ---------- helpers ----------


def _norm(s): return ("" if s is None else str(s)).strip()


def _desc_key(row): return _norm(row.get("brand")), _norm(row.get("fabric_name")), _norm(row.get("colour"))


def _q2(x: str):
    try: return Decimal(str(x).strip() or "0").quantize(Decimal("0.01"))
    except InvalidOperation: return Decimal("0.00")


def _tomorrow_ddmmyyyy(): return (datetime.today() + timedelta(days=1)).strftime("%d/%m/%Y")


def colour_for_parts(p3: str) -> str:
    raw = _norm(p3)
    if not raw: return ""
    if raw.lower() == "to be confirmed": return "Colour To Be Confirmed"
    # keep "Specified below" literal; otherwise Title Case
    return raw if raw.lower() == "specified below" else raw.title()


def colour_for_description(p3: str) -> str:
    raw = _norm(p3)
    return "" if raw.lower() == "specified below" else colour_for_parts(raw)


def rebuild_description(product_name, brand, fabric, colour):
    parts = [product_name.strip(), _norm(brand).title(), _norm(fabric).title(), colour_for_description(colour)]
    return " ".join([p for p in parts if p])


# ---------- master sources ----------
def fetch_master_from_sheets(app):
    from services.google_sheets_service import GoogleSheetsService
    svc = GoogleSheetsService()
    sheet_id = app.config["spreadsheets"]["master_curtain_fabric_list"]["id"]
    sheet_range = app.config["spreadsheets"]["master_curtain_fabric_list"]["range"]
    rows = svc.fetch_sheet_data(sheet_id, sheet_range)

    # header map
    hdr = {col.strip(): i for i, col in enumerate(rows[0])}
    need = ["Brand Name","Fabric Name","Colour","Width (cm)","Direction","Vertical Pattern Repeat Size (cm)",
            "Cost to DD per metre ROLL (ex GST)","Proposed NEW Price"]
    for c in need:
        if c not in hdr: raise RuntimeError(f"Missing column in master sheet: {c}")

    master = {}
    for r in rows[1:]:
        b, f, c = r[hdr["Brand Name"]], r[hdr["Fabric Name"]], r[hdr["Colour"]]
        key = _desc_key({"brand": b, "fabric_name": f, "colour": c})
        if not all(key): continue
        master[key] = {
            "brand": b, "fabric_name": f, "colour": c,
            "width_cm": r[hdr["Width (cm)"]] or "",
            "direction": r[hdr["Direction"]] or "",
            "repeat_cm": r[hdr["Vertical Pattern Repeat Size (cm)"]] or "",
            "sell_sqm": r[hdr["Proposed NEW Price"]] or "0",
            "cost_sqm": r[hdr["Cost to DD per metre ROLL (ex GST)"]] or "0",
        }
    return master


def fetch_master_from_db(db):
    # Use your existing Unleashed staging (adjust field names if needed)
    cur = db.execute_query("""
        SELECT
            Brand                AS brand,
            FabricName           AS fabric_name,
            Colour               AS colour,
            WidthCm              AS width_cm,
            Direction            AS direction,
            VerticalRepeatCm     AS repeat_cm,
            SellSQM              AS sell_sqm,
            CostSQM              AS cost_sqm,
            InventoryGroupCode   AS inventory_group_code
        FROM unleashed_products
        WHERE LOWER(TRIM(ProductGroup))='curtain'
          AND LOWER(TRIM(ProductSubGroup)) != 'ignore'
    """)
    master = {}
    for r in cur.fetchall():
        key = _desc_key(r)
        if not all(key): continue
        master[key] = dict(r)
    return master


# ---------- BUZ sources ----------
def fetch_buz_items(db):
    from services.buz_inventory_items import get_current_buz_fabrics
    rows = get_current_buz_fabrics(db)  # rows from inventory_items for CRTWT/CRTNT
    # group by (brand,fabric,colour); remember a preferred code per desc
    from collections import defaultdict
    by_desc, preferred_code = defaultdict(list), {}
    for row in rows:
        k = (_norm(row["DescnPart1"]), _norm(row["DescnPart2"]), _norm(row["DescnPart3"]))
        by_desc[k].append(row)
        if k not in preferred_code and _norm(row.get("Code")):
            preferred_code[k] = _norm(row["Code"])
    return by_desc, preferred_code


def fetch_buz_pricing(db):
    from services.buz_inventory_pricing import get_current_buz_pricing
    return get_current_buz_pricing(db)  # {InventoryCode: row}


# ---------- compare ----------
def compare_items(master_by_desc, buz_by_desc):
    mkeys, bkeys = set(master_by_desc), set(buz_by_desc)
    new_descs = [master_by_desc[k] for k in (mkeys - bkeys)]
    removed = [buz_by_desc[k][0] for k in (bkeys - mkeys)]  # representative
    updated = []
    # for shared desc keys, check non-key fields (width/repeat/direction)
    for k in (mkeys & bkeys):
        m, b = master_by_desc[k], buz_by_desc[k][0]
        bw_cm = (float(b["CustomVar2"])/10.0) if _norm(b.get("CustomVar2")) else None
        br_cm = (float(b["CustomVar1"])/10.0) if _norm(b.get("CustomVar1")) else None
        bd    = _norm(b.get("CustomVar3"))
        diffs = []
        try:
            if _norm(m.get("width_cm")) and bw_cm is not None and float(m["width_cm"]) != float(bw_cm): diffs.append("width")
        except: pass
        try:
            if _norm(m.get("repeat_cm")) and br_cm is not None and float(m["repeat_cm"]) != float(br_cm): diffs.append("repeat")
        except: pass
        if _norm(m.get("direction")) and bd and _norm(m["direction"])[0].upper() != bd.strip()[:1].upper(): diffs.append("direction")
        if diffs: updated.append(m | {"_diffs": diffs})
    return new_descs, updated, removed


# ---------- item/pricing changes ----------
def build_item_changes(new_descs, updated_descs, removed_rows):
    changes = {}
    def group_for(item):
        g = _norm(item.get("inventory_group_code"))
        return g if g in CURTAIN_TABS else "CRTNT"

    # Adds
    for m in new_descs:
        grp = group_for(m)
        row = {
            "PkId": "", "Operation": "A",
            "Supplier": _norm(m["brand"]),
            "DescnPart1": _norm(m["brand"]),
            "DescnPart2": _norm(m["fabric_name"]),
            "DescnPart3": colour_for_parts(m["colour"]),  # keep “Specified below”
            "CustomVar2": str(int(float(m["width_cm"])*10)) if _norm(m.get("width_cm")) else "",
            "CustomVar1": str(int(float(m["repeat_cm"])*10)) if _norm(m.get("repeat_cm")) else "",
            "CustomVar3": _norm(m.get("direction"))[:1].upper() if _norm(m.get("direction")) else "",
        }
        changes.setdefault(grp, []).append(row)

    # Edits
    for m in updated_descs:
        grp = group_for(m)
        row = {
            "Operation": "E",
            "Supplier": _norm(m["brand"]),
            "DescnPart1": _norm(m["brand"]),
            "DescnPart2": _norm(m["fabric_name"]),
            "DescnPart3": colour_for_parts(m["colour"]),
            "CustomVar2": str(int(float(m["width_cm"])*10)) if _norm(m.get("width_cm")) else "",
            "CustomVar1": str(int(float(m["repeat_cm"])*10)) if _norm(m.get("repeat_cm")) else "",
            "CustomVar3": _norm(m.get("direction"))[:1].upper() if _norm(m.get("direction")) else "",
        }
        changes.setdefault(grp, []).append(row)

    # Deletes
    for b in removed_rows:
        grp = _norm(b.get("inventory_group_code") or "CRTNT")
        row = dict(b); row["Operation"] = "D"
        changes.setdefault(grp, []).append(row)

    return changes


def build_pricing_changes(master_by_desc, buz_pricing_by_code, desc_to_code):
    changes = {}
    for k, m in master_by_desc.items():
        code = desc_to_code.get((_norm(m["brand"]), _norm(m["fabric_name"]), _norm(m["colour"])))
        if not code:  # no item yet
            continue
        sheet_sell, sheet_cost = _q2(m.get("sell_sqm")), _q2(m.get("cost_sqm"))
        last = buz_pricing_by_code.get(code)
        if last:
            last_sell = _q2(last.get("SellLMWide") or last.get("SellSQM"))
            last_cost = _q2(last.get("CostLMWide") or last.get("CostSQM"))
            changed = (sheet_sell != last_sell) or (sheet_cost != last_cost)
        else:
            changed = True
        if not changed:
            continue
        grp = _norm(m.get("inventory_group_code") or "CRTNT")
        changes.setdefault(grp, []).append({
            "PkId": "", "Operation": "A",
            "InventoryCode": code,
            "Description": rebuild_description("Curtain", m["brand"], m["fabric_name"], m["colour"]),
            "DateFrom": _tomorrow_ddmmyyyy(),
            "SellLMWide": f"{sheet_sell:.2f}", "SellLMHeight": f"{sheet_sell:.2f}",
            "CostLMWide": f"{sheet_cost:.2f}", "CostLMHeight": f"{sheet_cost:.2f}",
        })
    return changes


# ---------- orchestrator ----------
def run_curtain_fabric_sync_db(app, db, use_google_sheet=True):
    from services.buz_inventory_items import create_inventory_workbook_creator
    from services.buz_inventory_pricing import create_pricing_workbook_creator

    master = fetch_master_from_sheets(app) if use_google_sheet else fetch_master_from_db(db)
    buz_by_desc, desc_to_code = fetch_buz_items(db)
    buz_pricing_by_code = fetch_buz_pricing(db)

    new_descs, updated_descs, removed = compare_items(master, buz_by_desc)
    item_changes = build_item_changes(new_descs, updated_descs, removed)

    items_creator = create_inventory_workbook_creator(app)
    items_creator.populate_workbook(item_changes)
    items_creator.auto_fit_columns()
    items_path = "items_upload.xlsx"
    items_creator.save_workbook(items_path)

    # Pricing: 2dp compare, tomorrow date; new-fabric pricing handled when items are added
    pricing_changes = build_pricing_changes(master, buz_pricing_by_code, desc_to_code)
    pricing_creator = create_pricing_workbook_creator(app)
    pricing_creator.populate_workbook(pricing_changes)
    pricing_creator.auto_fit_columns()
    pricing_path = "pricing_upload.xlsx"
    pricing_creator.save_workbook(pricing_path)

    return {
        "items_file": items_path,
        "pricing_file": pricing_path,
        "summary": {
            "new_items": len(new_descs),
            "updated_items": len(updated_descs),
            "removed_items": len(removed),
            "pricing_changes": {k: len(v) for k, v in pricing_changes.items()},
        },
    }
