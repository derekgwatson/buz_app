# app/routes/customer_automation.py
from __future__ import annotations

import asyncio
import logging
import threading
import uuid
from flask import Blueprint, current_app, render_template, request, jsonify, redirect, url_for

from services.auth import auth
from services.job_service import create_job, update_job, get_job
from services.database import create_db_manager

logger = logging.getLogger(__name__)

customer_automation_bp = Blueprint("customer_automation", __name__, url_prefix="/tools/customer-automation")


@customer_automation_bp.route("/", methods=["GET"])
@auth.login_required
def index():
    """Customer automation entry point"""
    return render_template("customer_automation.html")


@customer_automation_bp.route("/add-from-zendesk", methods=["POST"])
@auth.login_required
def add_from_zendesk():
    """
    Start customer addition from Zendesk ticket

    Expects:
        ticket_id: Zendesk ticket number
        headless: (optional) Run browser in headless mode (default: true)
    """
    ticket_id = request.form.get("ticket_id", "").strip()
    headless = request.form.get("headless", "false").lower() in ("true", "1", "yes")

    if not ticket_id:
        return jsonify({"error": "Ticket ID is required"}), 400

    try:
        ticket_id = int(ticket_id)
    except ValueError:
        return jsonify({"error": "Ticket ID must be a number"}), 400

    # Create job
    job_id = uuid.uuid4().hex
    db_path = current_app.config["database"]

    db = create_db_manager(db_path)
    create_job(job_id, db=db)

    def run_automation():
        """Background thread to run customer automation"""
        db = create_db_manager(db_path)
        try:
            update_job(job_id, 5, f"Starting automation for ticket #{ticket_id}", db=db)

            # Import and run the automation
            from services.buz_customer_automation import add_customer_from_zendesk_ticket

            def job_callback(pct: int, message: str):
                """Update job progress"""
                update_job(job_id, pct, message, db=db)

            # Run async function in new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    add_customer_from_zendesk_ticket(
                        ticket_id=ticket_id,
                        headless=headless,
                        job_update_callback=job_callback
                    )
                )
            finally:
                loop.close()

            # Build result summary
            result_dict = result.to_dict()
            summary_lines = []

            if result.user_existed:
                if result.user_reactivated:
                    summary_lines.append(f"✓ User was reactivated: {result.user_email}")
                else:
                    summary_lines.append(f"✓ User already exists: {result.user_email}")
            else:
                if result.customer_existed:
                    summary_lines.append(f"✓ Found existing customer: {result.customer_name}")
                elif result.customer_created:
                    summary_lines.append(f"✓ Created customer: {result.customer_name}")

                if result.user_created:
                    summary_lines.append(f"✓ Created user: {result.user_email}")

            summary = "\n".join(summary_lines)

            update_job(
                job_id,
                pct=100,
                message="Automation completed successfully",
                done=True,
                result={
                    "summary": summary,
                    "ticket_id": ticket_id,
                    "customer_name": result.customer_name,
                    "user_email": result.user_email,
                    "steps": result.steps,
                    **result_dict
                },
                db=db
            )

        except Exception as e:
            logger.exception(f"Customer automation failed for ticket #{ticket_id}")

            # Check if this is a CustomerAutomationError with result steps
            from services.buz_customer_automation import CustomerAutomationError
            result_steps = None
            if isinstance(e, CustomerAutomationError) and hasattr(e, 'result'):
                result_steps = e.result.steps

            update_job(
                job_id,
                pct=0,
                message=f"Error: {str(e)}",
                error=str(e),
                done=True,
                result={
                    "error": str(e),
                    "steps": result_steps if result_steps else []
                },
                db=db
            )

    # Start background thread
    thread = threading.Thread(target=run_automation, daemon=True)
    thread.start()

    return jsonify({"job_id": job_id})


@customer_automation_bp.route("/job/<job_id>", methods=["GET"])
def job_status(job_id):
    """Get job status"""
    db_path = current_app.config["database"]
    db = create_db_manager(db_path)
    job = get_job(job_id, db=db)

    if not job:
        return jsonify({"error": "Job not found"}), 404

    return jsonify(job)


@customer_automation_bp.route("/add-user", methods=["GET"])
def add_user_form():
    """Show form for adding a user to an existing customer"""
    is_dev = current_app.config.get("DEBUG", False) or current_app.config.get("ENV") == "development"
    return render_template("customer_automation_add_user.html", is_dev=is_dev)


@customer_automation_bp.route("/add-user", methods=["POST"])
def add_user():
    """
    Start user addition for existing customer

    Expects:
        existing_user_email: Email of existing user (to find customer)
        first_name: New user's first name
        last_name: New user's last name
        email: New user's email
        phone: New user's phone (optional)
        buz_instances: Comma-separated list or multiple values
        headless: (optional) Run browser in headless mode (default: true)
    """
    existing_user_email = request.form.get("existing_user_email", "").strip()
    first_name = request.form.get("first_name", "").strip()
    last_name = request.form.get("last_name", "").strip()
    email = request.form.get("email", "").strip()
    phone = request.form.get("phone", "").strip() or None
    # Default to headless=True, but allow override in dev mode
    headless = request.form.get("headless", "true").lower() in ("true", "1", "yes")

    # Get buz_instances - can be comma-separated or multiple form values
    buz_instances_raw = request.form.getlist("buz_instances")
    if not buz_instances_raw:
        buz_instances_raw = [request.form.get("buz_instances", "")]

    # Parse and clean buz instances
    buz_instances = []
    for item in buz_instances_raw:
        if "," in item:
            buz_instances.extend([x.strip() for x in item.split(",") if x.strip()])
        elif item.strip():
            buz_instances.append(item.strip())

    # Validation
    errors = []
    if not existing_user_email:
        errors.append("Existing user email is required")
    if not first_name:
        errors.append("First name is required")
    if not last_name:
        errors.append("Last name is required")
    if not email:
        errors.append("Email is required")
    if not buz_instances:
        errors.append("At least one Buz instance is required")

    if errors:
        return jsonify({"error": ". ".join(errors)}), 400

    # Create job
    job_id = uuid.uuid4().hex
    db_path = current_app.config["database"]

    db = create_db_manager(db_path)
    create_job(job_id, db=db)

    def run_automation():
        """Background thread to run user addition automation"""
        db = create_db_manager(db_path)
        try:
            update_job(job_id, 5, f"Starting user addition automation", db=db)

            # Import and run the automation
            from services.buz_customer_automation import add_user_for_existing_customer, AddUserData

            def job_callback(pct: int, message: str):
                """Update job progress"""
                update_job(job_id, pct, message, db=db)

            # Create user data object
            user_data = AddUserData(
                existing_user_email=existing_user_email,
                first_name=first_name,
                last_name=last_name,
                email=email,
                buz_instances=buz_instances,
                phone=phone
            )

            # Run async function in new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    add_user_for_existing_customer(
                        user_data=user_data,
                        headless=headless,
                        job_update_callback=job_callback
                    )
                )
            finally:
                loop.close()

            # Build result summary
            result_dict = result.to_dict()
            summary_lines = []

            if result.user_existed:
                if result.user_reactivated:
                    summary_lines.append(f"✓ User was reactivated: {result.user_email}")
                else:
                    summary_lines.append(f"✓ User already exists: {result.user_email}")
            else:
                if result.user_created:
                    summary_lines.append(f"✓ Created user: {result.user_email}")
                    summary_lines.append(f"✓ Linked to customer: {result.customer_name}")

            summary = "\n".join(summary_lines)

            update_job(
                job_id,
                pct=100,
                message="User addition completed successfully",
                done=True,
                result={
                    "summary": summary,
                    "customer_name": result.customer_name,
                    "user_email": result.user_email,
                    "steps": result.steps,
                    **result_dict
                },
                db=db
            )

        except Exception as e:
            logger.exception(f"User addition automation failed")

            # Check if this is a CustomerAutomationError with result steps
            from services.buz_customer_automation import CustomerAutomationError
            result_steps = None
            if isinstance(e, CustomerAutomationError) and hasattr(e, 'result'):
                result_steps = e.result.steps

            update_job(
                job_id,
                pct=0,
                message=f"Error: {str(e)}",
                error=str(e),
                done=True,
                result={
                    "error": str(e),
                    "steps": result_steps if result_steps else []
                },
                db=db
            )

    # Start background thread
    thread = threading.Thread(target=run_automation, daemon=True)
    thread.start()

    return jsonify({"job_id": job_id})
