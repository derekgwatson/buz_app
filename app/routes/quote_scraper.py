# app/routes/quote_scraper.py
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

quote_scraper_bp = Blueprint("quote_scraper", __name__, url_prefix="/tools/quote-scraper")


@quote_scraper_bp.route("/", methods=["GET"])
@auth.login_required
def index():
    """Quote scraper entry point"""
    return render_template("quote_scraper.html")


@quote_scraper_bp.route("/scrape", methods=["POST"])
@auth.login_required
def scrape_quote():
    """
    Start quote history scraping

    Expects:
        order_id: Order/quote ID (GUID)
        account: (optional) Buz account name (default: watsonblinds)
        headless: (optional) Run browser in headless mode (default: true)
    """
    order_id = request.form.get("order_id", "").strip()
    account = request.form.get("account", "watsonblinds").strip()
    headless = request.form.get("headless", "true").lower() in ("true", "1", "yes")

    if not order_id:
        return jsonify({"error": "Order ID is required"}), 400

    # Validate that storage state exists
    storage_state_path = Path(f".secrets/buz_storage_state_{account}.json")
    if not storage_state_path.exists():
        return jsonify({
            "error": f"Auth storage state not found for account '{account}'. "
                     f"Please run: python tools/buz_auth_bootstrap.py {account}"
        }), 400

    # Create job
    job_id = uuid.uuid4().hex
    db_path = current_app.config["database"]

    db = create_db_manager(db_path)
    create_job(job_id, db=db)

    def run_scraper():
        """Background thread to run quote scraper"""
        db = create_db_manager(db_path)
        try:
            update_job(job_id, 5, f"Starting scraper for order {order_id}", db=db)

            # Import and run the scraper
            from services.buz_quote_scraper import scrape_quote_history

            def job_callback(pct: int, message: str):
                """Update job progress"""
                update_job(job_id, pct, message, db=db)

            # Run async function in new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    scrape_quote_history(
                        order_id=order_id,
                        storage_state_path=storage_state_path,
                        headless=headless,
                        progress_callback=job_callback
                    )
                )
            finally:
                loop.close()

            # Build result
            result_dict = result.to_dict()
            summary = f"Scraped {result.total_entries} history entries"

            if result.errors:
                summary += f" (with {len(result.errors)} errors)"

            update_job(
                job_id,
                pct=100,
                message="Scraping completed successfully",
                done=True,
                result={
                    "summary": summary,
                    "order_id": result.order_id,
                    "total_entries": result.total_entries,
                    "entries": result_dict["entries"],
                    "errors": result.errors
                },
                db=db
            )

        except Exception as e:
            logger.exception(f"Quote scraping failed for order {order_id}")

            update_job(
                job_id,
                pct=0,
                message=f"Error: {str(e)}",
                error=str(e),
                done=True,
                result={
                    "error": str(e)
                },
                db=db
            )

    # Start background thread
    thread = threading.Thread(target=run_scraper, daemon=True)
    thread.start()

    return jsonify({"job_id": job_id})


@quote_scraper_bp.route("/job/<job_id>", methods=["GET"])
@auth.login_required
def job_status(job_id):
    """Get job status"""
    db_path = current_app.config["database"]
    db = create_db_manager(db_path)
    job = get_job(job_id, db=db)

    if not job:
        return jsonify({"error": "Job not found"}), 404

    return jsonify(job)
