# app/routes/discount_groups.py
# PEP 8 compliant

from __future__ import annotations
from services.auth import auth

import os
import tempfile
from werkzeug.utils import secure_filename

from services.config_service import ConfigManager
from services.discount_groups_sync import DiscountGroupsSync
from services.google_sheets_service import GoogleSheetsService

# app/routes/discount_groups.py
from flask import (
    Blueprint, request, jsonify, send_file, render_template, current_app, url_for
)


discount_groups_bp = Blueprint("discount_groups", __name__)


# -------------------------
# Routes
# -------------------------
@discount_groups_bp.route("/discount-groups/manual", methods=["GET"])
@auth.login_required
def manual_steps():
    """
    Shows per-customer manual steps with live Buz links,
    derived from the Google Sheet tabs.
    """
    cfg = ConfigManager()

    current_app.logger.info("CFG PATH? %s", getattr(cfg, "resolved_path", None))
    current_app.logger.info("TOP KEYS: %s", list(getattr(cfg, "config", {}).keys()))
    current_app.logger.info("discount_grid block: %s", getattr(cfg, "config", {}).get("discount_grid"))
    current_app.logger.info("dot get sheet id: %r", cfg.get("discount_grid.google_sheet_id", default=None))

    sync = DiscountGroupsSync(cfg)

    try:
        current_app.logger.info("DiscountGroups: using sheet_id=%s", sync.sheet_id)
        tabs = sync.list_customer_tabs(GoogleSheetsService())
        if not tabs:
            return jsonify({"ok": False,
                            "error": "No tabs found. Check sheet ID and share the sheet with the service account."}), 400
        rows = sync.get_manual_steps(tabs)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Unable to read Google Sheet tabs: {exc}"}), 400

    return render_template(
        "discount_groups/manual.html",
        rows=rows,
        create_url=sync.url_create_group,
        find_url=sync.url_find_customer,
    )


@discount_groups_bp.route("/discount-groups/build", methods=["POST"])
def build_discount_grid():
    """
    Accepts the base Discount Grid (.xlsm or .xlsx) and returns a summary page
    with a download link for the updated file.
    """
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "file is required"}), 400

    f = request.files["file"]
    filename = secure_filename(f.filename or "grid.xlsx")
    _, ext = os.path.splitext(filename)
    if ext.lower() not in (".xlsm", ".xlsx"):
        return jsonify({"ok": False, "error": "Please upload an .xlsm or .xlsx file"}), 400

    with tempfile.NamedTemporaryFile(prefix="grid_in_", suffix=ext, delete=False) as tmp_in:
        f.save(tmp_in)
        in_path = tmp_in.name

    out_path = in_path.replace("grid_in_", "grid_out_")
    cfg = ConfigManager()
    sync = DiscountGroupsSync(cfg)

    try:
        result = sync.build(in_path, out_path)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400

    return render_template(
        "discount_groups/summary.html",
        totals=result.get("totals", {"cells_changed": 0}),
        customers=result.get("customers", []),
        changes_sample=result.get("changes_sample", []),
        file_path=result["output_file"],
        download_url=url_for("discount_groups_bp.download_grid"),
    )


@discount_groups_bp.route("/discount-groups/download", methods=["GET"])
@auth.login_required
def download_grid():
    """
    Streams the updated workbook to the user.
    """
    fp = request.args.get("fp")
    if not fp or not os.path.exists(fp):
        return jsonify({"ok": False, "error": "File not found"}), 404

    dl_name = "DiscountGrid_UPDATED.xlsx" if fp.lower().endswith(".xlsx") else "DiscountGrid_UPDATED.xlsm"
    return send_file(fp, as_attachment=True, download_name=dl_name)
