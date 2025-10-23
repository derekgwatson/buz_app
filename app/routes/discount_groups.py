# app/routes/discount_groups.py
from __future__ import annotations
from services.auth import auth

import os
import tempfile
from werkzeug.utils import secure_filename

from services.config_service import ConfigManager
from services.discount_groups_sync import DiscountGroupsSync
from services.google_sheets_service import GoogleSheetsService

from flask import (
    Blueprint, request, jsonify, send_file, render_template, current_app, url_for, after_this_request
)

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

_DOWNLOAD_TTL = 15 * 60            # 15 minutes

discount_groups_bp = Blueprint("discount_groups", __name__)


def _temp_dir() -> str:
    return tempfile.gettempdir()  # e.g. /tmp on Linux, %TEMP% on Windows


def _mk_temp_output_path(ext: str) -> str:
    fd, path = tempfile.mkstemp(prefix="discount_grid_out_", suffix=ext, dir=_temp_dir())
    os.close(fd)  # we only needed the filename
    return path


def _serializer():
    return URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt="discount-grid-dl")


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
        sheet_url=sync.get_sheet_url(),
        invdg_create_url=sync.url_discount_group_grid,
    )


@discount_groups_bp.route("/discount-groups/build", methods=["POST"])
@auth.login_required
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

    out_path = _mk_temp_output_path(ext)
    cfg = ConfigManager()
    sync = DiscountGroupsSync(cfg)

    try:
        result = sync.build(in_path, out_path)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    finally:
        try:
            os.remove(in_path)
        except OSError:
            pass

    cells_changed = int(result.get("totals", {}).get("cells_changed", 0) or 0)

    download_url = None
    if cells_changed > 0:
        # Use the path we just created (should match result["output_file"])
        out_fp = result["output_file"]
        dl_name = (
            "DiscountGrid_UPDATED.xlsm"
            if out_fp.lower().endswith(".xlsm")
            else "DiscountGrid_UPDATED.xlsx"
        )
        basename = os.path.basename(out_fp)

        token = _serializer().dumps({"file": basename, "name": dl_name})
        download_url = url_for("discount_groups.download_grid", token=token)

    else:
        try:
            os.remove(result["output_file"])
        except OSError:
            pass

    return render_template(
        "discount_groups/summary.html",
        totals=result.get("totals", {"cells_changed": 0}),
        customers=result.get("customers", []),
        changes_sample=result.get("changes_sample", []),
        download_url=download_url,
        cells_changed=cells_changed,
    )


@discount_groups_bp.route("/discount-groups/download/<token>", methods=["GET"])
@auth.login_required
def download_grid(token: str):
    s = _serializer()
    try:
        meta = s.loads(token, max_age=_DOWNLOAD_TTL)
    except (SignatureExpired, BadSignature):
        return jsonify({"ok": False, "error": "Download expired or not found"}), 404

    basename = meta.get("file")
    dl_name = meta.get("name") or (basename or "DiscountGrid_UPDATED.xlsx")

    if not basename:
        return jsonify({"ok": False, "error": "Invalid token"}), 400

    path = os.path.join(_temp_dir(), basename)
    if not os.path.exists(path):
        return jsonify({"ok": False, "error": "Download expired or not found"}), 404

    @after_this_request
    def _cleanup(response):
        try:
            os.remove(path)
        except OSError:
            pass
        return response

    mime = (
        "application/vnd.ms-excel.sheet.macroEnabled.12"
        if path.lower().endswith(".xlsm")
        else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    return send_file(path, as_attachment=True, download_name=dl_name, mimetype=mime)
