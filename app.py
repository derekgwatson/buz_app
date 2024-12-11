import json
import os
from flask import Flask, render_template, request, url_for, flash, redirect, send_file, g
import time

from data_processing import (search_items_by_supplier_code, insert_unleashed_data,
                             get_inventory_group_codes,  
                             db_delete_records_by_inventory_group, db_delete_inventory_group, 
                             get_table_row_count, get_unique_inventory_group_count, 
                             db_delete_items_not_in_unleashed)

from process_buz_workbooks import process_workbook
                             
from database import get_db_connection, close_db_connection, DatabaseManager, init_db
from services.google_sheets_service import GoogleSheetsService
from helper import generate_multiple_unique_ids
from constants import EXPECTED_HEADERS_ITEMS, EXPECTED_HEADERS_PRICING
from group_options_check import (extract_codes_from_excel_flat_dedup, map_inventory_items_to_tabs,
                                 filter_inventory_items, extract_duplicate_codes_with_locations)
from backorders import process_inventory_backorder_with_services
from services.remove_old_items import delete_deprecated_items_request
from services.excel import OpenPyXLFileHandler
from services.config_service import ConfigManager

import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

app = Flask(__name__)

# Set a secret key for session management
app.secret_key = os.urandom(24)  # Generate a random secret key
app.config['UPLOAD_FOLDER'] = 'uploads'


@app.cli.command("init-db")
def initialize_database():
    """Initialize the database tables."""
    get_db_connection()
    init_db(DatabaseManager(g.db))
    print("Database initialized.")


@app.before_request
def before_request():
    """
    Initialize and close database connection for each request
    """
    get_db_connection()

    """Track the start time of each request."""
    g.start_time = time.time()


@app.after_request
def after_request(response):
    if hasattr(g, 'start_time'):
        duration = time.time() - g.start_time
        g.request_duration = f"{duration:.3f} seconds"

        if response.content_type == "text/html; charset=utf-8":
            response_data = response.get_data(as_text=True)
            if "[[ request_duration ]]" in response_data:
                response_data = response_data.replace("[[ request_duration ]]", g.request_duration)
                response.set_data(response_data)

    return response


@app.teardown_request
def teardown_request(exception):
    close_db_connection(exception)


@app.route('/debug')
def debug():
    """Debug route to check g variables."""
    return f"g.start_time: {getattr(g, 'start_time', 'None')}, g.request_duration: {getattr(g, 'request_duration', 'None')}"


@app.route('/')
def homepage():
    return render_template('home.html')


@app.route('/upload')
def upload_form():
    return render_template(
        'upload.html',
        inventory_count=get_table_row_count('inventory_items'),
        pricing_count=get_table_row_count('pricing_data'),
        unleashed_count=get_table_row_count('unleashed_products'),
        inventory_group_count=get_unique_inventory_group_count()
    )
    

@app.route('/upload', methods=['POST'])
def upload_file():
    print("Upload request received.")  # Debug: Log when the function is called

    # Check for files in the request
    inventory_file = request.files.get('inventory_file')
    pricing_file = request.files.get('pricing_file')
    unleashed_file = request.files.get('unleashed_file')

    # Initialize a list to store which files are uploaded
    uploaded_files = []

    if inventory_file:
        print(f"Received inventory_file: {inventory_file.filename}")  # Debug: Log file name
        inventory_file_path = os.path.join(app.config['UPLOAD_FOLDER'], inventory_file.filename)
        print(f"Saving inventory file to: {inventory_file_path}")  # Debug: Log the save path
        inventory_file.save(inventory_file_path)
        print("Inventory file saved successfully.")  # Debug: Confirmation of save
        process_workbook(
            file_handler=OpenPyXLFileHandler(file_path=inventory_file_path),
            table_name='inventory_items',
            expected_headers=EXPECTED_HEADERS_ITEMS,
            header_row=2
        )
        uploaded_files.append('inventory_file')

    if pricing_file:
        print(f"Received pricing_file: {pricing_file.filename}")  # Debug: Log file name
        pricing_file_path = os.path.join(app.config['UPLOAD_FOLDER'], pricing_file.filename)
        print(f"Saving pricing file to: {pricing_file_path}")  # Debug: Log the save path
        pricing_file.save(pricing_file_path)
        print("Pricing file saved successfully.")  # Debug: Confirmation of save
        process_workbook(
            file_handler=OpenPyXLFileHandler(file_path=pricing_file_path),
            table_name='pricing_data',
            expected_headers=EXPECTED_HEADERS_PRICING,
            header_row=1
        )
        uploaded_files.append('pricing_file')

    if unleashed_file:
        print(f"Received unleashed_file: {unleashed_file.filename}")  # Debug: Log file name
        unleashed_file_path = os.path.join(app.config['UPLOAD_FOLDER'], unleashed_file.filename)
        print(f"Saving unleashed file to: {unleashed_file_path}")  # Debug: Log the save path
        unleashed_file.save(unleashed_file_path)
        print("Unleashed file saved successfully.")  # Debug: Confirmation of save
        insert_unleashed_data(unleashed_file_path)
        uploaded_files.append('unleashed_file')

    if not uploaded_files:
        print("No files to upload.")  # Debug: Log when no files are present
        return 'No files to upload'

    flash(f'Files successfully uploaded and data stored in the database')
    return redirect(url_for('upload_form'))


@app.route('/upload_inventory_group_codes', methods=['POST'])
def upload_inventory_group_codes():
    if 'group_codes_file' not in request.files:
        flash('No file part')
        return redirect(url_for('manage_inventory_groups'))

    group_codes_file = request.files['group_codes_file']

    if group_codes_file.filename == '':
        flash('No selected file')
        return redirect(url_for('manage_inventory_groups'))

    # Read the uploaded text file and add each line as an inventory group code
    codes_added = []
    if group_codes_file:
        lines = group_codes_file.read().decode('utf-8').splitlines()  # Read lines from the file
        for line in lines:
            group_code = line.strip()  # Remove any surrounding whitespace
            if group_code:  # Only add non-empty lines
                db_manager.insert_item('inventory_group_codes', {'group_code': group_code})                
                codes_added.append(group_code)

    flash(f'Added inventory group codes: {", ".join(codes_added)}')
    return redirect(url_for('manage_inventory_groups'))


# Route to search for items by supplier product code
@app.route('/search', methods=['GET', 'POST'])
def search():
    results = []
    if request.method == 'POST':
        code = request.form['code']
        results = search_items_by_supplier_code(code)
    return render_template('search.html', results=results)
    

@app.route('/manage_inventory_groups', methods=['GET', 'POST'])
def manage_inventory_groups():
    if request.method == 'POST':
        db_manager = DatabaseManager(g.db)
        # Handle adding a new inventory group code
        new_group_code = request.form['new_group_code']
        db_manager.insert_item("inventory_group_codes", new_group_code)
        flash(f'Added inventory group code: {new_group_code}')
        return redirect(url_for('manage_inventory_groups'))

    allowed_groups = get_inventory_group_codes()  # Fetch only the codes
    logging.INFO(f"allowed groups: {allowed_groups}")
    return render_template('manage_inventory_groups.html', allowed_groups=allowed_groups)


@app.route('/delete_inventory_group/<string:inventory_group_code>', methods=['POST'])
def delete_inventory_group(inventory_group_code):
    db_manager = DatabaseManager(g.db)

    # Delete inventory group code
    db_manager.delete_item("inventory_group_codes", {"group_code": inventory_group_code})

    # Delete records from items and pricing tables
    db_delete_records_by_inventory_group(inventory_group_code)
    
    # Optionally, delete the inventory group code from the allowed list as well
    db_delete_inventory_group(inventory_group_code)

    flash(f'Deleted all records for inventory group code: {inventory_group_code}')
    return redirect(url_for('manage_inventory_groups'))


@app.route('/download/<filename>')
def download_file(filename):
    uploads_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
    file_path = os.path.join(uploads_dir, filename)
    return send_file(file_path, as_attachment=True)


@app.route('/delete_items_not_in_unleashed', methods=['POST'])
def delete_items_not_in_unleashed():
    db_delete_items_not_in_unleashed()
    flash('All inventory items not found in the unleashed products and with a blank SupplierProductCode ' +
          'have been deleted.')
    return redirect(url_for('get_items_not_in_unleashed'))  # Redirect to the report page


@app.route('/get_items_not_in_unleashed', methods=['GET', 'POST'])
def get_items_not_in_unleashed():
    if request.method == 'POST':
        output_file = delete_deprecated_items_request(request)

        # Process the workbook
        return render_template('delete_items_not_in_unleashed.html', output_file=output_file)
    else:
        return render_template('delete_items_not_in_unleashed.html')


@app.route('/get_group_option_codes', methods=['GET', 'POST'])
def get_group_option_codes():
    if request.method == 'POST':
        # Initialize the file handler for the input workbook
        g_file_handler = OpenPyXLFileHandler(file=request.files.get('group_options_file'))
        items = extract_codes_from_excel_flat_dedup(g_file_handler)
        g_file_handler = OpenPyXLFileHandler(file=request.files.get('inventory_items_file'))
        items = map_inventory_items_to_tabs(g_file_handler, items)
        items = filter_inventory_items(items)

        return render_template('get_group_option_codes.html', codes=items)

    else:
        return render_template('get_group_option_codes.html')


@app.route('/get_duplicate_codes', methods=["GET", "POST"])
def get_group_codes_duplicated():
    if request.method == 'POST':
        # Initialize the file handler for the input workbook
        g_file_handler = OpenPyXLFileHandler(file=request.files.get('group_options_file'))
        items = extract_codes_from_excel_flat_dedup(g_file_handler)
        items = extract_duplicate_codes_with_locations(items)

        return render_template('get_duplicate_codes.html', codes=items)

    else:
        return render_template('get_duplicate_codes.html')


@app.route('/generate_codes', methods=["GET", "POST"])
def generate_codes():
    if request.method == "POST":
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


@app.route('/generate_backorder_file', methods=["GET", "POST"])
def generate_backorder_file():
    # Get the directory where the script is located
    base_dir = os.path.dirname(os.path.abspath(__file__))
    uploads_dir = os.path.join(base_dir, 'uploads')

    # Ensure the uploads directory exists
    if not os.path.exists(uploads_dir):
        os.makedirs(uploads_dir)

    if request.method == "POST":
        # Load the existing config
        config_manager = ConfigManager()

        # Update config with user-provided values
        spreadsheet_id_old = config_manager.config.get('backorder_spreadsheet_id')
        spreadsheet_id = request.args.get('spreadsheet_id')
        spreadsheet_range_old = config_manager.config.get('backorder_spreadsheet_range')
        spreadsheet_range = request.args.get('spreadsheet_range')
        config_manager.update_config_backorder(spreadsheet_id=spreadsheet_id, spreadsheet_range=spreadsheet_range)
        config_updated = spreadsheet_range_old != spreadsheet_range or spreadsheet_id_old != spreadsheet_id
        if config_updated:
            flash('Config updated', 'success')

        if 'inventory_items_file' in request.files:
            original_filename = os.path.join(uploads_dir, 'original_file.xlsx')
            upload_filename = os.path.join(uploads_dir, 'upload_file.xlsx')

            inventory_items_file = request.files.get('inventory_items_file')
            if inventory_items_file and \
                    inventory_items_file.filename.strip() != '' and \
                    inventory_items_file.content_length > 0:
                g_file_handler = OpenPyXLFileHandler(file=request.files.get('inventory_items_file'))
                g_sheets_service = GoogleSheetsService(json_file=os.path.join(os.path.dirname(__file__), 'static',
                                                                              'buz-app-439103-b6ae046c4723.json'))

                # Save updated config back to the file
                process_inventory_backorder_with_services(
                    _file_handler=g_file_handler,
                    _sheets_service=g_sheets_service,
                    spreadsheet_id=spreadsheet_id,
                    range_name=spreadsheet_range,
                    original_filename=original_filename,
                    upload_filename=upload_filename,
                    header_row=2,
                )
                return render_template(
                    'generate_backorder_file.html',
                    original_filename=original_filename,
                    upload_filename=upload_filename
                )
            else:
                if not config_updated:
                    flash('Inventory files upload file is empty.', 'warning')
                return render_template(
                    'generate_backorder_file.html',
                    spreadsheet_id = config_manager.config.get('backorder_spreadsheet_id'),
                    spreadsheet_range= config_manager.config.get('backorder_spreadsheet_range'),
                )
        else:
            flash('No file uploaded.', 'warning')
    else:
        # Load config for defaults
        with open("config.json") as f:
            config = json.load(f)
        return render_template(
            'generate_backorder_file.html',
            spreadsheet_id=config['backorder_spreadsheet_id'],
            spreadsheet_range=config['backorder_spreadsheet_range']
        )


if __name__ == '__main__':
    # Initialize your API wrappers
    app.run(debug=True)
