# app/routes/user_management.py
from __future__ import annotations

import asyncio
import logging
import threading
import uuid
from flask import Blueprint, current_app, render_template, request, jsonify

from services.auth import auth
from services.job_service import create_job, update_job, get_job
from services.database import create_db_manager

logger = logging.getLogger(__name__)

user_management_bp = Blueprint("user_management", __name__, url_prefix="/tools/user-management")


@user_management_bp.route("/", methods=["GET"])
@auth.login_required
def index():
    """User management entry point"""
    return render_template("user_management.html", is_development=current_app.config.get('DEBUG', False))


@user_management_bp.route("/start-review", methods=["POST"])
@auth.login_required
def start_review():
    """
    Start user management review across selected orgs.

    Expects:
        orgs: (required) List of org keys to process
        headless: (optional) Run browser in headless mode (default: true, forced in production)
    """
    headless = request.form.get("headless", "true").lower() in ("true", "1", "yes")
    # Force headless mode in production (DEBUG=False)
    if not current_app.config.get('DEBUG', False):
        headless = True

    # Get selected orgs
    selected_orgs = request.form.getlist("orgs")
    if not selected_orgs:
        return jsonify({"error": "At least one organization must be selected"}), 400

    # Validate org keys
    valid_orgs = ['canberra', 'tweed', 'dd', 'bay', 'shoalhaven', 'wagga']
    invalid_orgs = [org for org in selected_orgs if org not in valid_orgs]
    if invalid_orgs:
        return jsonify({"error": f"Invalid organizations: {', '.join(invalid_orgs)}"}), 400

    # Create job
    job_id = uuid.uuid4().hex
    db_path = current_app.config["database"]

    db = create_db_manager(db_path)
    create_job(job_id, db=db)

    def run_review():
        """Background thread to run user management review"""
        db = create_db_manager(db_path)
        try:
            org_names = ', '.join([org.title() for org in selected_orgs])
            update_job(job_id, 5, f"Starting user management review for: {org_names}", db=db)

            # Import and run the review
            from services.buz_user_management import review_users_all_orgs
            from services.user_management_comparison import build_user_comparison

            def job_callback(pct: int, message: str):
                """Update job progress"""
                update_job(job_id, pct, message, db=db)

            # Run async function in new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    review_users_all_orgs(
                        headless=headless,
                        selected_orgs=selected_orgs,
                        job_update_callback=job_callback
                    )
                )
            finally:
                loop.close()

            # Build comparison
            update_job(job_id, 90, "Building user comparison table", db=db)
            comparison = build_user_comparison(result)

            # Build summary
            total_users = sum(len(org.users) for org in result.orgs)
            summary_lines = [
                f"✓ Scraped users from {len(result.orgs)} orgs",
                f"✓ Found {total_users} total user records",
            ]

            for org in result.orgs:
                active_count = len([u for u in org.users if u.is_active])
                inactive_count = len([u for u in org.users if not u.is_active])
                summary_lines.append(f"  - {org.org_name}: {active_count} active, {inactive_count} inactive")

            summary = "\n".join(summary_lines)

            update_job(
                job_id,
                pct=100,
                message="Review completed successfully",
                done=True,
                result={
                    "summary": summary,
                    "comparison": comparison.to_dict(),
                    "raw_result": result.to_dict()
                },
                db=db
            )

        except Exception as e:
            logger.exception(f"User management review failed")
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


@user_management_bp.route("/job/<job_id>", methods=["GET"])
@auth.login_required
def job_status(job_id):
    """Get job status"""
    db_path = current_app.config["database"]
    db = create_db_manager(db_path)
    job = get_job(job_id, db=db)

    if not job:
        return jsonify({"error": "Job not found"}), 404

    return jsonify(job)


@user_management_bp.route("/latest", methods=["GET"])
@auth.login_required
def get_latest_result():
    """Get the most recent successful user management review"""
    db_path = current_app.config["database"]
    db = create_db_manager(db_path)

    # Query for the most recent completed user management job
    # We can identify user management jobs by looking at the result structure
    query = """
        SELECT id, status, pct, log, error, result, updated_at
        FROM jobs
        WHERE status = 'completed'
        AND result IS NOT NULL
        AND result LIKE '%comparison%'
        AND result LIKE '%user%'
        ORDER BY updated_at DESC
        LIMIT 1
    """

    rows = db.execute_query(query)

    if not rows:
        return jsonify({"error": "No cached data found"}), 404

    row = rows[0]
    import json

    job = {
        "id": row[0],
        "status": row[1],
        "pct": row[2],
        "log": json.loads(row[3]) if row[3] else [],
        "error": row[4],
        "result": json.loads(row[5]) if row[5] else None,
        "updated_at": row[6]
    }

    return jsonify(job)


@user_management_bp.route("/toggle-user-status", methods=["POST"])
@auth.login_required
def toggle_user_status():
    """Toggle a user's active/inactive status"""
    data = request.get_json()
    org_key = data.get('org_key')
    user_email = data.get('user_email')

    if not org_key or not user_email:
        return jsonify({"error": "org_key and user_email are required"}), 400

    # Validate org key
    valid_orgs = ['canberra', 'tweed', 'dd', 'bay', 'shoalhaven', 'wagga']
    if org_key not in valid_orgs:
        return jsonify({"error": f"Invalid org_key: {org_key}"}), 400

    headless = current_app.config.get('DEBUG', False) == False  # Force headless in production

    try:
        from services.buz_user_management import toggle_user_active_status

        # Run async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                toggle_user_active_status(
                    org_key=org_key,
                    user_email=user_email,
                    headless=headless
                )
            )
        finally:
            loop.close()

        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 500

    except Exception as e:
        logger.exception(f"Error toggling user status")
        return jsonify({"error": str(e)}), 500
