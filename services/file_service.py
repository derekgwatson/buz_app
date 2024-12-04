import os
from services.excel import OpenPyXLFileHandler


def validate_file(file):
    if not file or file.filename.strip() == '' or file.content_length == 0:
        return False
    return True


def process_backorder_file(file, sheets_service, config, uploads_dir):
    original_filename = os.path.join(uploads_dir, 'original_file.xlsx')
    upload_filename = os.path.join(uploads_dir, 'upload_file.xlsx')

    file_handler = OpenPyXLFileHandler(file=file)
    spreadsheet_id = config['backorder_spreadsheet_id']
    range_name = config['backorder_spreadsheet_range']

    file_handler.save(original_filename)
    file_handler.save(upload_filename)  # Example save logic

    sheets_service.sync_with_google_sheet(spreadsheet_id, range_name)  # Example integration
