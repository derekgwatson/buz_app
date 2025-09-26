from __future__ import annotations

from flask import Blueprint, current_app, render_template, request, redirect, flash
from werkzeug.utils import secure_filename
import os
import shutil
import tempfile
from markupsafe import Markup
from services.google_sheets_service import GoogleSheetsService
from services.lead_times.links import tab_url
from services.config_bridge import get_cfg, where_cfg
from services.lead_times.api import run_publish

lead_times_bp = Blueprint("lead_times", __name__, url_prefix="/tools/lead_times")


# app/routes/lead_times.py
def _base_output_dir() -> str:
    # Prefer global app config first (these are NOT under lead_times)
    base = (
        current_app.config.get("UPLOAD_OUTPUT_DIR")
        or current_app.config.get("upload_folder")
        # then allow lead_times-scoped values (for people who moved them)
        or get_cfg("UPLOAD_OUTPUT_DIR")
        or get_cfg("upload_folder")
        or "uploads"
    )
    if not os.path.isabs(base):
        base = os.path.join(current_app.root_path, base)
    os.makedirs(base, exist_ok=True)
    return base


@lead_times_bp.route("/", methods=["GET", "POST"])
def start():
    # Pull the whole lead_times dict from file-backed config (preferred),
    # falling back to app.config["lead_times"] if needed.
    lead_times_cfg = get_cfg() or current_app.config.get("lead_times")
    if not lead_times_cfg:
        raise RuntimeError(
            "Lead Times config missing. Call get_cfg() with NO arguments "
            "(it is already rooted at 'lead_times'), or set current_app.config['lead_times'].\n"
            f"Debug: {where_cfg()}"
        )

    if request.method == "GET":
        cutoff_tab = get_cfg("lead_times", "cutoffs", "tab")
        return render_template("lead_times_start.html", cutoff_tab=cutoff_tab)

    detailed = request.files.get("detailed_template")
    summary = request.files.get("summary_template")
    if not detailed or not summary:
        flash("Please upload both Detailed and Summary .xlsm files.")
        return redirect(request.url)

    tmpdir = tempfile.mkdtemp(prefix="lead_times_")
    try:
        d_path = os.path.join(tmpdir, secure_filename(detailed.filename))
        s_path = os.path.join(tmpdir, secure_filename(summary.filename))
        detailed.save(d_path)
        summary.save(s_path)

        svc = GoogleSheetsService()
        scope = request.form.getlist("scope") or ["CANBERRA", "REGIONAL"]

        res = run_publish(
            gsheets_service=svc,
            lead_times_cfg=lead_times_cfg,
            detailed_template_path=d_path,
            summary_template_path=s_path,
            save_dir=_base_output_dir(),
            scope=tuple(scope),
        )

        return render_template(
            "lead_times_result.html",
            warnings=res["warnings"],
            html_canberra=res["html"].get("canberra", ""),
            html_regional=res["html"].get("regional", ""),
            files=res["files"],
        )

    except ValueError as exc:
        flash(Markup(str(exc)), "error")
        return redirect(request.url)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@lead_times_bp.get("/open")
def open_sheet():
    """302 to the correct Google Sheet tab using your config shape."""
    svc = GoogleSheetsService()

    kind = (request.args.get("kind") or "lead").lower()
    tab_param = (request.args.get("tab") or "").strip()

    if kind == "cutoff":
        sheet_id = get_cfg("cutoffs","sheet_id")
        tab_name = tab_param or get_cfg("cutoffs","tab")
        return redirect(tab_url(svc, sheet_id, tab_name), code=302)

    # default: lead times (note: config key is 'lead_times_ss')
    sheet_id = get_cfg("lead_times", "lead_times_ss", "sheet_id")
    tabs = get_cfg("lead_times", "lead_times_ss", "tabs") or {}
    if tab_param.lower() in ("canberra", "regional"):
        tab_name = tabs[tab_param.lower()]
    else:
        tab_name = tab_param or tabs["canberra"]
    return redirect(tab_url(svc, sheet_id, tab_name), code=302)
