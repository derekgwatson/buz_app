from flask import Blueprint, current_app
import os
from flask import render_template, request, url_for, flash, redirect, send_file, g, send_from_directory
from datetime import timezone
from services.group_options_check import extract_codes_from_excel_flat_dedup
from services.excel import OpenPyXLFileHandler
from services.config_service import ConfigManager
import logging
from services.auth import auth


# Create Blueprint
main_routes = Blueprint('main_routes', __name__)


@auth.verify_password
def verify_password(username, password):
    users = current_app.config['USERS']
    if username in users and users[username] == password:
        return username


@main_routes.route('/debug')
def debug():
    """Debug route to check g variables."""
    return f"g.start_time: {getattr(g, 'start_time', 'None')}, g.request_duration: {getattr(g, 'request_duration', 'None')}"


@main_routes.route('/')
@auth.login_required
def homepage():
    return render_template('home.html')


@main_routes.route('/upload', methods=['GET', 'POST'])
@auth.login_required
def upload_route():
    from services.upload import upload, parse_headers, init_last_upload_times

    # Initialize last upload times
    last_upload_times = init_last_upload_times(g.db)

    if request.method == 'POST':
        # Retrieve files from the request
        inventory_file = request.files.get('inventory_file')
        pricing_file = request.files.get('pricing_file')
        unleashed_file = request.files.get('unleashed_file')

        # Parse headers
        inventory_file_expected_headers, inventory_file_db_fields = parse_headers(
            current_app.config, "buz_inventory_item_file"
        )
        pricing_file_expected_headers, pricing_file_db_fields = parse_headers(
            current_app.config, "buz_pricing_file"
        )
        unleashed_file_expected_headers, _ = parse_headers(
            current_app.config, "unleashed_fields"
        )

        # Process uploaded files
        uploaded_files = upload(
            db_manager=g.db,
            inventory_file=inventory_file,
            inventory_file_expected_headers=inventory_file_expected_headers,
            inventory_file_db_fields=inventory_file_db_fields,
            pricing_file=pricing_file,
            pricing_file_expected_headers=pricing_file_expected_headers,
            pricing_file_db_fields=pricing_file_db_fields,
            unleashed_file=unleashed_file,
            unleashed_file_expected_headers=unleashed_file_expected_headers,
            upload_folder=current_app.config['upload_folder'],
            invalid_pkid=current_app.config['invalid_pkid']
        )

        if uploaded_files:
            last_upload_times.update(uploaded_files)
            flash('Files successfully uploaded and data stored in the database')
        else:
            flash('No files to upload')

    # Render the upload page
    from services.data_processing import (
        get_unique_inventory_group_count,
        get_table_row_count
    )

    return render_template(
        'upload.html',
        inventory_count=get_table_row_count(g.db, 'inventory_items'),
        pricing_count=get_table_row_count(g.db, 'pricing_data'),
        unleashed_count=get_table_row_count(g.db, 'unleashed_products'),
        inventory_group_count=get_unique_inventory_group_count(g.db),
        last_upload_times=last_upload_times
    )


@main_routes.app_template_filter('datetimeformat')
def datetimeformat(value):
    if value:
        # Convert to UTC and format as ISO 8601
        return value.astimezone(timezone.utc).isoformat() + "Z"
    return "N/A"


@main_routes.route('/upload_inventory_groups', methods=['POST'])
@auth.login_required
def upload_inventory_groups():
    if 'groups_file' not in request.files:
        flash('No file part')
        return redirect(url_for('main_routes.manage_inventory_groups'))

    groups_file = request.files['groups_file']

    if groups_file.filename == '':
        flash('No selected file')
        return redirect(url_for('main_routes.manage_inventory_groups'))

    # Read the uploaded text file and add each line as an inventory group code
    codes_added = []
    if groups_file:
        lines = groups_file.read().decode('utf-8').splitlines()  # Read lines from the file
        for line in lines:
            group_code = line.strip()  # Remove any surrounding whitespace
            if group_code:  # Only add non-empty lines
                g.db.insert_item('inventory_group_codes', {'group_code': group_code})
                codes_added.append(group_code)

    flash(f'Added inventory groups: {", ".join(codes_added)}')
    return redirect(url_for('main_routes.manage_inventory_groups'))


# Route to search for items by supplier product code
@main_routes.route('/search', methods=['GET', 'POST'])
@auth.login_required
def search():
    from services.data_processing import search_items_by_supplier_code

    results = []
    if request.method == 'POST':
        code = request.form['code']
        results = search_items_by_supplier_code(db_manager=g.db, code=code)
    return render_template('search.html', results=results)


@main_routes.route('/manage_inventory_groups', methods=['GET', 'POST'])
@auth.login_required
def manage_inventory_groups():
    from services.data_processing import get_inventory_groups

    if request.method == 'POST':
        # Handle adding a new inventory group code
        new_group_code = request.form['new_group_code']
        new_group_description = request.form['new_group_description']
        g.db.insert_item("inventory_groups", new_group_code, new_group_description)
        flash(f'Added inventory group: {new_group_description} ({new_group_code})')
        return redirect(url_for('main_routes.manage_inventory_groups'))

    inventory_groups = get_inventory_groups(g.db)  # Fetch only the codes
    logging.info(f"allowed groups: {inventory_groups}")
    return render_template('manage_inventory_groups.html', inventory_groups=inventory_groups)


@main_routes.route('/delete_inventory_group/<string:inventory_group_code>', methods=['POST'])
@auth.login_required
def delete_inventory_group(inventory_group_code):
    from services.data_processing import db_delete_inventory_group
    from services.data_processing import db_delete_records_by_inventory_group

    # Delete inventory group code
    g.db.delete_item("inventory_groups", {"group_code": inventory_group_code})

    # Delete records from items and pricing tables
    db_delete_records_by_inventory_group(inventory_group_code)

    # Optionally, delete the inventory group code from the allowed list as well
    db_delete_inventory_group(inventory_group_code)

    flash(f'Deleted all records for inventory group code: {inventory_group_code}')
    return redirect(url_for('main_routes.manage_inventory_groups'))


@main_routes.route('/download/<filename>')
@auth.login_required
def download_file(filename):
    file_path = os.path.join(current_app.config['upload_folder'], filename)
    return send_file(file_path, as_attachment=True)


@main_routes.route('/get_items_not_in_unleashed', methods=['GET', 'POST'])
@auth.login_required
def get_items_not_in_unleashed():
    from services.remove_old_items import remove_old_items

    if request.method == 'POST':
        output_file = 'output_file.xlsx'
        if remove_old_items(db_manager=g.db, app_config=current_app.config, output_file=output_file):
            return render_template('delete_items_not_in_unleashed.html', output_file=output_file)
        flash("No items found to delete!")

    return render_template('delete_items_not_in_unleashed.html')


@main_routes.route('/get_group_option_codes', methods=['GET', 'POST'])
@auth.login_required
def get_group_option_codes():
    if request.method == 'POST':
        from services.group_options_check import map_inventory_items_to_tabs
        from services.group_options_check import filter_inventory_items

        # Initialize the file handler for the input workbook
        g_file_handler = OpenPyXLFileHandler.from_file_like(file=request.files.get('group_options_file'))
        items = extract_codes_from_excel_flat_dedup(g_file_handler)
        g_file_handler = OpenPyXLFileHandler.from_file_like(file=request.files.get('inventory_items_file'))
        items = map_inventory_items_to_tabs(g_file_handler, items)
        items = filter_inventory_items(items)

        return render_template('get_group_option_codes.html', codes=items)

    return render_template('get_group_option_codes.html')


@main_routes.route('/get_duplicate_codes', methods=["GET", "POST"])
@auth.login_required
def get_duplicate_codes():
    if request.method == 'POST':
        from services.group_options_check import extract_duplicate_codes_with_locations

        # Initialize the file handler for the input workbook
        g_file_handler = OpenPyXLFileHandler.from_file_like(file=request.files.get('group_options_file'))
        items = extract_codes_from_excel_flat_dedup(g_file_handler)
        items = extract_duplicate_codes_with_locations(items)

        return render_template('get_duplicate_codes.html', codes=items)

    else:
        return render_template('get_duplicate_codes.html')


@main_routes.route('/generate_codes', methods=["GET", "POST"])
@auth.login_required
def generate_codes():
    if request.method == "POST":
        from services.helper import generate_multiple_unique_ids

        try:
            count = int(request.form.get("count", 1))
            if count <= 0:
                raise ValueError("Count must be greater than 0.")
            ids = generate_multiple_unique_ids(count)
        except ValueError as e:
            flash('Error {e}')
            return

        return render_template('show_generated_ids.html', ids=ids)

    else:
        return render_template('show_generated_ids.html')


@main_routes.route('/generate_backorder_file', methods=["GET", "POST"])
@auth.login_required
def generate_backorder_file():
    from services.google_sheets_service import GoogleSheetsService
    from services.config_service import SpreadsheetConfigUpdater

    if request.method == "POST":
        # Update config with user-provided values
        spreadsheet_config_manager = SpreadsheetConfigUpdater(ConfigManager())

        spreadsheet_id = request.form.get('spreadsheet_id')
        spreadsheet_range = request.form.get('spreadsheet_range')
        if spreadsheet_config_manager.update_spreadsheet_config("backorders", spreadsheet_id, spreadsheet_range):
            flash('Config updated', 'success')

        # Resolve the path to the JSON credentials file using __file__
        credentials_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '..', 'credentials', 'buz-app-439103-b6ae046c4723.json'
        )
        g_sheets_service = GoogleSheetsService(json_file=credentials_path)

        from services.backorders import process_inventory_backorder_with_services

        # Save updated config back to the file
        upload_wb, original_wb = process_inventory_backorder_with_services(
            _db_manager=g.db,
            _sheets_service=g_sheets_service,
            spreadsheet_id=spreadsheet_id,
            range_name=spreadsheet_range
        )
        original_filename = 'original_file.xlsx'
        upload_filename = 'upload_file.xlsx'

        upload_wb.save(current_app.config["upload_folder"] + '/' + upload_filename)
        original_wb.save(current_app.config["upload_folder"] + '/' + original_filename)

        return render_template(
            'generate_backorder_file.html',
            original_filename=original_filename,
            upload_filename=upload_filename,
        )

    return render_template(
        'generate_backorder_file.html',
        spreadsheet_id=current_app.config["spreadsheets"]["backorders"]["id"],
        spreadsheet_range=current_app.config["spreadsheets"]["backorders"]["range"]
    )


@main_routes.route('/robots.txt')
def robots_txt():
    return send_from_directory(current_app.static_folder, 'robots.txt')


@main_routes.route('/get_buz_items_by_supplier_codes', methods=['GET', 'POST'])
@auth.login_required
def get_buz_items_by_supplier_codes():
    from services.buz_items_by_supplier_code import process_buz_items_by_supplier_codes

    if request.method == 'POST':
        uploaded_file = request.files.get('file')
        supplier_codes_input = request.form.get('supplier_codes')

        if not uploaded_file or not supplier_codes_input:
            return "Error: File or supplier codes missing.", 400

        # Process multi-line supplier codes input
        supplier_codes = [code.strip() for code in supplier_codes_input.splitlines() if code.strip()]

        if not uploaded_file.filename.endswith(('.xlsx', '.xlsm')):
            logging.warning("Only .xlsx or .xlsm files are supported.")
            flash("Only .xlsx or .xlsm files are supported.")
        else:
            try:
                excel = OpenPyXLFileHandler().from_file_like(uploaded_file)
                filtered_excel = process_buz_items_by_supplier_codes(
                    excel,
                    supplier_codes,
                    current_app.config["headers"]["buz_inventory_item_file"]
                )
                if filtered_excel:
                    return send_file(
                        filtered_excel,
                        as_attachment=True,
                        download_name='filtered_items.xlsx',
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
                else:
                    logging.warning("No items found that match the list of supplier codes")
                    flash("Nothing found")
            except Exception as e:
                logging.error(f"Error: {e}")
                if current_app.debug:
                    raise e
                else:
                    flash(f"Error: {e}")

    return render_template('get_buz_items_by_supplier_codes.html')


@main_routes.route("/get_matching_buz_items", methods=["GET", "POST"])
@auth.login_required
def get_matching_buz_items():
    from services.get_matching_buz_items import process_matching_buz_items

    if request.method == "POST":
        # Get the uploaded files
        first_file = request.files["first_file"]
        second_file = request.files["second_file"]

        # Save the uploaded files
        first_path = os.path.join(current_app.config['upload_folder'], first_file.filename)
        second_path = os.path.join(current_app.config['upload_folder'], second_file.filename)
        first_file.save(first_path)
        second_file.save(second_path)

        # Process the files
        output_path = os.path.join("static", "filtered_output.xlsx")
        matches_found = process_matching_buz_items(first_path, second_path, output_path)

        if not matches_found:
            flash("No matches found in any sheets.")
            return render_template("get_matching_buz_items.html")

        # Provide the output file for download
        return send_file(output_path, as_attachment=True)

    return render_template("get_matching_buz_items.html")


@main_routes.route("/sync_pricing", methods=["GET", "POST"])
@auth.login_required
def sync_pricing():
    from services.sync_pricing import get_pricing_changes

    if request.method == "POST":
        filenames = get_pricing_changes(
            db_manager=g.db,
            upload_folder=current_app.config['upload_folder'],
            pricing_fields=current_app.config["headers"]["buz_pricing_file"],
            wastage_percentages=current_app.config["wastage_percentages"]
        )
        return render_template("sync_pricing.html", filenames=filenames)
    return render_template("sync_pricing.html")


@main_routes.route('/fabrics/create', methods=['GET'])
def create_fabric():
    """
    Render the form to create a new fabric.
    """
    return render_template('fabric_create.html')
