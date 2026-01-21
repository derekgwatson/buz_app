from __future__ import annotations

from flask import Blueprint, current_app, render_template, request, redirect, flash
from werkzeug.utils import secure_filename
import os
import shutil
import tempfile
from markupsafe import Markup
from typing import List

from services.google_sheets_service import GoogleSheetsService
from services.lead_times.links import sheet_url
from services.config_bridge import get_cfg, where_cfg
from services.lead_times.api import run_publish
from services.excel_safety import save_workbook_gracefully


lead_times_bp = Blueprint("lead_times", __name__, url_prefix="/tools/lead_times")


def _base_output_dir() -> str:
    """
    Prefer global app config first (these are NOT under lead_times).
    """
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


def _compose_html(scope: str, store: str, body_html: str) -> str:
    lead_cfg = get_cfg() or {}
    html_cfg = (lead_cfg.get("html") or {}) if isinstance(lead_cfg, dict) else {}

    legacy_prefix = lead_cfg.get("html_prefix", "")
    legacy_suffix = lead_cfg.get("html_suffix", "")

    global_prefix = html_cfg.get("prefix", "") or legacy_prefix
    global_suffix = html_cfg.get("suffix", "") or legacy_suffix
    prefix_by_scope = html_cfg.get("prefix_by_scope", {}) or {}
    suffix_by_scope = html_cfg.get("suffix_by_scope", {}) or {}

    store_overrides = html_cfg.get("store_overrides", {}) or {}
    store_cfg = store_overrides.get((store or "").lower(), {}) or {}

    s = (scope or "").lower()
    store_key = (store or "").lower()

    parts = [
        str(store_cfg.get("prefix", "") or ""),            # 0 store prefix
        str(prefix_by_scope.get(s, "") or ""),             # 1 scope prefix
        str(global_prefix or ""),                          # 2 global prefix
        str(body_html or ""),                              # 3 BODY (unchanged)
        str(global_suffix or ""),                          # 4 global suffix
        str(suffix_by_scope.get(s, "") or ""),             # 5 scope suffix
        str(store_cfg.get("suffix", "") or ""),            # 6 store suffix
    ]

    def _apply_tokens(text: str) -> str:
        return (
            text.replace("{{STORE}}", store_key.title())
                .replace("{{store}}", store_key)
                .replace("{{STORE_UPPER}}", store_key.upper())
        )

    out: List[str] = []
    for i, p in enumerate(parts):
        out.append(_apply_tokens(p) if i != 3 else p)  # don’t touch BODY
    return "".join(out).strip()


@lead_times_bp.route("/", methods=["GET", "POST"])
def start():
    """
    Lead Times entry point. Accepts two Excel uploads and produces HTML + files.
    """
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
        cutoff_tab_canberra = get_cfg("cutoffs", "tabs", "canberra")
        cutoff_tab_regional = get_cfg("cutoffs", "tabs", "regional")
        return render_template(
            "lead_times_start.html",
            cutoff_tab_canberra=cutoff_tab_canberra,
            cutoff_tab_regional=cutoff_tab_regional)

    detailed = request.files.get("detailed_template")
    summary = request.files.get("summary_template")
    if not detailed or not summary:
        flash("Please upload both Detailed and Summary Excel files.")
        return redirect(request.url)

    tmpdir = tempfile.mkdtemp(prefix="lead_times_")
    try:
        d_path = os.path.join(tmpdir, secure_filename(detailed.filename or "detailed.xlsx"))
        s_path = os.path.join(tmpdir, secure_filename(summary.filename or "summary.xlsx"))
        detailed.save(d_path)
        summary.save(s_path)

        svc = GoogleSheetsService()
        scope = request.form.get("scope")

        res = run_publish(
            gsheets_service=svc,
            lead_times_cfg=lead_times_cfg,
            detailed_template_path=d_path,
            summary_template_path=s_path,
            save_dir=_base_output_dir(),
            scope=scope,
        )

        return render_template(
            "lead_times_result.html",
            warnings=res.get("warnings", []),
            html=res["html"],
            files=res.get("files", []),
            scopes=scope,
            is_review_mode=res.get("is_review_mode", False),
            review_reasons=res.get("review_reasons", []),
        )

    except ValueError as exc:
        flash(Markup(str(exc)), "error")
        return redirect(request.url)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@lead_times_bp.get("/open")
def open_sheet():
    kind = (request.args.get("kind") or "lead").strip().lower()

    if kind == "cutoff":
        sheet_id = get_cfg("cutoffs", "sheet_id")
        return redirect(sheet_url(sheet_id), code=302)

    # default: lead times (config key is 'lead_times_ss')
    sheet_id = get_cfg("lead_times_ss", "sheet_id")
    return redirect(sheet_url(sheet_id), code=302)
