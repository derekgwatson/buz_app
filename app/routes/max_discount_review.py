# app/routes/max_discount_review.py
from __future__ import annotations

import asyncio
import logging
import threading
import uuid
from pathlib import Path
from flask import Blueprint, current_app, render_template, request, jsonify

from services.auth import auth
from services.job_service import create_job, update_job, get_job
from services.database import create_db_manager

logger = logging.getLogger(__name__)

max_discount_review_bp = Blueprint("max_discount_review", __name__, url_prefix="/tools/max-discount-review")


@max_discount_review_bp.route("/", methods=["GET"])
@auth.login_required
def index():
    """Max discount review entry point"""
    return render_template("max_discount_review.html")


@max_discount_review_bp.route("/start-review", methods=["POST"])
@auth.login_required
def start_review():
    """
    Start max discount review across all orgs.

    Expects:
        headless: (optional) Run browser in headless mode (default: true)
    """
    headless = request.form.get("headless", "true").lower() in ("true", "1", "yes")

    # Create job
    job_id = uuid.uuid4().hex
    db_path = current_app.config["database"]

    db = create_db_manager(db_path)
    create_job(job_id, db=db)

    # Create output directory for this job
    export_root = Path(current_app.config.get("EXPORT_ROOT", "exports"))
    output_dir = export_root / "max_discount_review" / job_id
    output_dir.mkdir(parents=True, exist_ok=True)

    def run_review():
        """Background thread to run max discount review"""
        db = create_db_manager(db_path)
        try:
            update_job(job_id, 5, "Starting max discount review", db=db)

            # Import and run the review
            from services.buz_max_discount_review import review_max_discounts_all_orgs
            from services.max_discount_comparison import build_max_discount_comparison

            def job_callback(pct: int, message: str):
                """Update job progress"""
                update_job(job_id, pct, message, db=db)

            # Run async function in new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    review_max_discounts_all_orgs(
                        output_dir=output_dir,
                        headless=headless,
                        job_update_callback=job_callback
                    )
                )
            finally:
                loop.close()

            # Build comparison
            update_job(job_id, 90, "Building comparison table", db=db)
            comparison = build_max_discount_comparison(result)

            # Build summary
            summary_lines = [
                f"✓ Downloaded and parsed inventory groups from {len(result.orgs)} orgs",
                f"✓ Found {comparison.to_dict()['summary']['total_products']} unique products",
                f"  - {comparison.to_dict()['summary']['matched_by_code']} matched by code",
                f"  - {comparison.to_dict()['summary']['matched_by_description']} matched by description"
            ]

            for org in result.orgs:
                summary_lines.append(f"  - {org.org_name}: {len(org.inventory_groups)} groups")

            summary = "\n".join(summary_lines)

            update_job(
                job_id,
                pct=100,
                message="Review completed successfully",
                done=True,
                result={
                    "summary": summary,
                    "comparison": comparison.to_dict(),
                    "raw_result": result.to_dict(),
                    "output_dir": str(output_dir)
                },
                db=db
            )

        except Exception as e:
            logger.exception(f"Max discount review failed")
            update_job(
                job_id,
                pct=0,
                message=f"Error: {str(e)}",
                error=str(e),
                done=True,
                result={"error": str(e)},
                db=db
            )

    # Start background thread
    thread = threading.Thread(target=run_review, daemon=True)
    thread.start()

    return jsonify({"job_id": job_id})


@max_discount_review_bp.route("/job/<job_id>", methods=["GET"])
@auth.login_required
def job_status(job_id):
    """Get job status"""
    db_path = current_app.config["database"]
    db = create_db_manager(db_path)
    job = get_job(job_id, db=db)

    if not job:
        return jsonify({"error": "Job not found"}), 404

    return jsonify(job)
