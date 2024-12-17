import os

from flask import Flask, render_template, request, url_for, flash, redirect, send_file, g, send_from_directory
import time

from services.group_options_check import extract_codes_from_excel_flat_dedup
from services.excel import OpenPyXLFileHandler
from services.config_service import ConfigManager

import logging
from services.database import init_db_command
import sqlite3
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

app = Flask(__name__)

load_dotenv()
app.secret_key = os.getenv("FLASK_SECRET", os.urandom(24))

# Configure database
app.config['DB_CONNECTOR'] = sqlite3.connect
app.config['DB_PARAMS'] = {'database': 'buz_data.db'}

# note, ConfigManager updates app.config, so we pass in app
ConfigManager(app)

# Register the CLI command
app.cli.add_command(init_db_command)    # type: ignore


@app.before_request
def before_request():
    """
    Initialize and close database connection for each request
    """
    from services.database import create_db_manager

    g.db = create_db_manager()

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
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()


@app.route('/debug')
def debug():
    """Debug route to check g variables."""
    return f"g.start_time: {getattr(g, 'start_time', 'None')}, g.request_duration: {getattr(g, 'request_duration', 'None')}"


@app.route('/')
def homepage():
    return render_template('home.html')


@app.route('/upload')
def upload_form():
    from services.data_processing import get_unique_inventory_group_count
    from services.data_processing import get_table_row_count

    return render_template(
        'upload.html',
        inventory_count=get_table_row_count(g.db, 'inventory_items'),
        pricing_count=get_table_row_count(g.db, 'pricing_data'),
        unleashed_count=get_table_row_count(g.db, 'unleashed_products'),
        inventory_group_count=get_unique_inventory_group_count(g.db)
    )
    

@app.route('/upload', methods=['POST'])
def upload_file():
    from services.data_processing import insert_unleashed_data
    from services.process_buz_workbooks import process_workbook
    from services.constants import EXPECTED_HEADERS_ITEMS, EXPECTED_HEADERS_PRICING

    # Check for files in the request
    inventory_file = request.files.get('inventory_file')
    pricing_file = request.files.get('pricing_file')
    unleashed_file = request.files.get('unleashed_file')

    # Initialize a list to store which files are uploaded
    uploaded_files = []

    if inventory_file:
        inventory_file_path = os.path.join(app.config['APP_CONFIG'].get('UPLOAD_FOLDER'), inventory_file.filename)
        inventory_file.save(inventory_file_path)
        process_workbook(
            file_handler=OpenPyXLFileHandler(file_path=inventory_file_path),
            table_name='inventory_items',
            expected_headers=EXPECTED_HEADERS_ITEMS,
            header_row=2
        )
        uploaded_files.append('inventory_file')

    if pricing_file:
        pricing_file_path = os.path.join(app.config['APP_CONFIG'].get('UPLOAD_FOLDER'), pricing_file.filename)
        pricing_file.save(pricing_file_path)
        process_workbook(
            file_handler=OpenPyXLFileHandler(file_path=pricing_file_path),
            table_name='pricing_data',
            expected_headers=EXPECTED_HEADERS_PRICING,
            header_row=1
        )
        uploaded_files.append('pricing_file')

    if unleashed_file:
        unleashed_file_path = os.path.join(app.config['APP_CONFIG'].get('UPLOAD_FOLDER'), unleashed_file.filename)
        unleashed_file.save(unleashed_file_path)
        insert_unleashed_data(unleashed_file_path)
        uploaded_files.append('unleashed_file')

    if not uploaded_files:
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
                app.config["DB_MANAGER"].insert_item('inventory_group_codes', {'group_code': group_code})
                codes_added.append(group_code)

    flash(f'Added inventory group codes: {", ".join(codes_added)}')
    return redirect(url_for('manage_inventory_groups'))


# Route to search for items by supplier product code
@app.route('/search', methods=['GET', 'POST'])
def search():
    from services.data_processing import search_items_by_supplier_code

    results = []
    if request.method == 'POST':
        code = request.form['code']
        results = search_items_by_supplier_code(code)
    return render_template('search.html', results=results)
    

@app.route('/manage_inventory_groups', methods=['GET', 'POST'])
def manage_inventory_groups():
    from services.data_processing import get_inventory_group_codes

    if request.method == 'POST':
        # Handle adding a new inventory group code
        new_group_code = request.form['new_group_code']
        app.config["DB_MANAGER"].insert_item("inventory_group_codes", new_group_code)
        flash(f'Added inventory group code: {new_group_code}')
        return redirect(url_for('manage_inventory_groups'))

    allowed_groups = get_inventory_group_codes()  # Fetch only the codes
    logging.INFO(f"allowed groups: {allowed_groups}")
    return render_template('manage_inventory_groups.html', allowed_groups=allowed_groups)


@app.route('/delete_inventory_group/<string:inventory_group_code>', methods=['POST'])
def delete_inventory_group(inventory_group_code):
    from services.data_processing import db_delete_inventory_group
    from services.data_processing import db_delete_records_by_inventory_group

    # Delete inventory group code
    app.config["DB_MANAGER"].delete_item("inventory_group_codes", {"group_code": inventory_group_code})

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
    from services.data_processing import db_delete_items_not_in_unleashed

    db_delete_items_not_in_unleashed()
    flash('All inventory items not found in the unleashed products and with a blank SupplierProductCode ' +
          'have been deleted.')
    return redirect(url_for('get_items_not_in_unleashed'))  # Redirect to the report page


@app.route('/get_items_not_in_unleashed', methods=['GET', 'POST'])
def get_items_not_in_unleashed():
    from services.remove_old_items import delete_deprecated_items_request

    if request.method == 'POST':
        output_file = delete_deprecated_items_request(request)

        # Process the workbook
        return render_template('delete_items_not_in_unleashed.html', output_file=output_file)
    else:
        return render_template('delete_items_not_in_unleashed.html')


@app.route('/get_group_option_codes', methods=['GET', 'POST'])
def get_group_option_codes():
    if request.method == 'POST':
        from services.group_options_check import map_inventory_items_to_tabs
        from services.group_options_check import filter_inventory_items

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
        from services.group_options_check import extract_duplicate_codes_with_locations

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


@app.route('/generate_backorder_file', methods=["GET", "POST"])
def generate_backorder_file():
    from services.google_sheets_service import GoogleSheetsService

    # Get the directory where the script is located
    base_dir = os.path.dirname(os.path.abspath(__file__))
    uploads_dir = os.path.join(base_dir, 'uploads')

    # Ensure the uploads directory exists
    if not os.path.exists(uploads_dir):
        os.makedirs(uploads_dir)

    if request.method == "POST":
        # Update config with user-provided values
        config_manager = ConfigManager()

        spreadsheet_id = request.form.get('spreadsheet_id')
        spreadsheet_range = request.form.get('spreadsheet_range')
        if config_manager.update_config_backorders(spreadsheet_id, spreadsheet_range):
            flash('Config updated', 'success')

        if 'inventory_items_file' in request.files:
            inventory_items_file = request.files.get('inventory_items_file')

            if inventory_items_file and \
                    inventory_items_file.filename.strip() != '' and \
                    len(inventory_items_file.read()) > 0:
                inventory_items_file.seek(0)
                g_file_handler = OpenPyXLFileHandler(file=inventory_items_file)
                g_sheets_service = GoogleSheetsService(json_file=os.path.join(os.path.dirname(__file__), 'credentials',
                                                                              'buz-app-439103-b6ae046c4723.json'))

                from services.backorders import process_inventory_backorder_with_services

                # Save updated config back to the file
                upload_wb, original_wb = process_inventory_backorder_with_services(
                    _file_handler=g_file_handler,
                    _sheets_service=g_sheets_service,
                    spreadsheet_id=spreadsheet_id,
                    range_name=spreadsheet_range,
                    header_row=2,
                )
                original_filename = 'original_file.xlsx'
                upload_filename = 'upload_file.xlsx'

                upload_wb.save(upload_filename)
                original_wb.save(original_filename)

                return render_template(
                    'generate_backorder_file.html',
                    original_filename=original_filename,
                    upload_filename=upload_filename,
                )
            else:
                return render_template(
                    'generate_backorder_file.html',
                    spreadsheet_id = app.config["spreadsheets.backorders.id"],
                    spreadsheet_range= app.config["spreadsheets.backorders.range"],
                )
        else:
            flash('No file uploaded.', 'warning')
    else:
        return render_template(
            'generate_backorder_file.html',
            spreadsheet_id=app.config["spreadsheets.backorders.id"],
            spreadsheet_range=app.config["spreadsheets.backorders.range"]
        )


@app.route('/robots.txt')
def robots_txt():
    return send_from_directory(app.static_folder, 'robots.txt')


@app.route('/get_buz_items_by_supplier_codes', methods=['GET', 'POST'])
def get_buz_items_by_supplier_codes():
    from services.buz_items_by_supplier_code import process_buz_items_by_supplier_codes

    if request.method == 'POST':
        uploaded_file = request.files.get('file')
        supplier_codes_input = request.form.get('supplier_codes')

        if not uploaded_file or not supplier_codes_input:
            return "Error: File or supplier codes missing.", 400

        # Process multi-line supplier codes input
        supplier_codes = [code.strip() for code in supplier_codes_input.splitlines() if code.strip()]

        if uploaded_file.filename.endswith(('.xlsx', '.xlsm')):
            try:
                filtered_excel = process_buz_items_by_supplier_codes(uploaded_file, supplier_codes)
                if filtered_excel:
                    return send_file(
                        filtered_excel,
                        as_attachment=True,
                        download_name='filtered_items.xlsx',
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
                else:
                    flash("Nothing found")
            except Exception as e:
                if app.debug:
                    raise e
                else:
                    flash(f"Error: {e}")

        else:
            flash("Error: Only .xlsx or .xlsm files are supported.")

    return render_template('get_buz_items_by_supplier_codes.html')


@app.route("/get_matching_buz_items", methods=["GET", "POST"])
def get_matching_buz_items():
    from services.get_matching_buz_items import process_matching_buz_items

    if request.method == "POST":
        # Get the uploaded files
        first_file = request.files["first_file"]
        second_file = request.files["second_file"]

        # Save the uploaded files
        first_path = os.path.join(app.config['APP_CONFIG'].get('UPLOAD_FOLDER'), first_file.filename)
        second_path = os.path.join(app.config['APP_CONFIG'].get('UPLOAD_FOLDER'), second_file.filename)
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


if __name__ == '__main__':
    # Initialize your API wrappers
    app.run(debug=True)
