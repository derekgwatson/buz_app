from __future__ import annotations

from flask import Blueprint, current_app, render_template, request, redirect, flash
from werkzeug.utils import secure_filename
import os
import shutil
import tempfile
from markupsafe import Markup
from services.google_sheets_service import GoogleSheetsService
from services.lead_times.api import run_publish
from services.lead_times.links import tab_url


lead_times_bp = Blueprint("lead_times", __name__, url_prefix="/tools/lead_times")


def _base_output_dir() -> str:
    base = (
        current_app.config.get("UPLOAD_OUTPUT_DIR")
        or current_app.config.get("upload_folder")
        or "uploads"
    )
    if not os.path.isabs(base):
        base = os.path.join(current_app.root_path, base)
    os.makedirs(base, exist_ok=True)
    return base


@lead_times_bp.route("/", methods=["GET", "POST"])
def start():
    if request.method == "GET":
        cutoff_tab = current_app.config["lead_times"]["cutoffs"]["tab"]
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
        cfg = current_app.config["lead_times"]
        scope = request.form.getlist("scope") or ["CANBERRA", "REGIONAL"]

        res = run_publish(
            gsheets_service=svc,
            lead_times_cfg=cfg,
            detailed_template_path=d_path,
            summary_template_path=s_path,
            save_dir=_base_output_dir(),
            scope=tuple(scope),
        )

        file_links = res["files"]  # basenames

        return render_template(
            "lead_times_result.html",
            warnings=res["warnings"],
            html_canberra=res["html"].get("canberra", ""),
            html_regional=res["html"].get("regional", ""),
            files=file_links,
        )

    except ValueError as exc:
        flash(Markup(str(exc)), "error")
        return redirect(request.url)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@lead_times_bp.get("/open")
def open_sheet():
    """302 to the correct Google Sheet tab using your config shape."""
    cfg = current_app.config["lead_times"]
    svc = GoogleSheetsService()

    kind = (request.args.get("kind") or "lead").lower()
    tab_param = (request.args.get("tab") or "").strip()

    if kind == "cutoff":
        sheet_id = cfg["cutoffs"]["sheet_id"]
        tab_name = tab_param or cfg["cutoffs"]["tab"]
        return redirect(tab_url(svc, sheet_id, tab_name), code=302)

    # default: lead times (note: config key is 'lead_times_ss')
    sheet_id = cfg["lead_times_ss"]["sheet_id"]
    tabs = cfg["lead_times_ss"]["tabs"]
    if tab_param.lower() in ("canberra", "regional"):
        tab_name = tabs[tab_param.lower()]
    else:
        tab_name = tab_param or tabs["canberra"]
    return redirect(tab_url(svc, sheet_id, tab_name), code=302)
