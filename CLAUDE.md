# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Flask web application for managing inventory, fabric synchronization, pricing, and lead times for Watson Blinds. The app integrates with:
- **Buz Manager** (blinds/curtains business management system) via web scraping and OData API
- **Unleashed Software** (inventory management) via REST API
- **Google Sheets** for data import/export and configuration

The app uses a SQLite database for local caching and job tracking, with extensive Excel file processing for bulk operations.

## Development Commands

### Running the Application

```bash
# Development mode (with debug enabled)
python run.py

# Production mode (via WSGI)
python wsgi.py
```

The app runs on Flask's default port (5000) in development mode.

### Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_fabrics.py

# Run tests with verbose output
pytest -v

# Run tests matching a pattern
pytest -k "test_database"
```

### Environment Setup

Required environment file: `.env` (see `.gitignore` for structure)
Required credentials:
- `credentials/service_account.json` - Google Sheets API credentials
- `credentials/unleashed.json` - Unleashed API credentials (api_id, api_key)

Environment variables:
- `FLASK_SECRET` - Flask session secret key
- `USERS` - JSON string of authorized users for basic auth
- `BUZ_LOGIN_BASE` - Buz Manager login endpoint (default: https://login.buzmanager.com)
- `GSHEETS_CREDENTIALS` or `GOOGLE_APPLICATION_CREDENTIALS` - Path to Google service account JSON

### Database

```bash
# Initialize database (creates tables, sets up schema)
flask init-db
```

The SQLite database is stored at `instance/buz_data.db` by default (configurable in `config.json`).

## Architecture

### Application Factory Pattern

The app uses Flask's application factory pattern in `app/__init__.py`:
- `create_app(config_name)` creates and configures the Flask app
- Configuration is loaded from both `config.py` classes and `config.json` (merged)
- Per-request database connections are managed via `g.db` in before/after request hooks
- Stale jobs (status='running') are automatically aborted on startup

### Blueprint Structure

Routes are organized into blueprints in `app/routes/`:
- `main_routes_bp` - Core inventory, upload, and sync operations
- `fabrics_bp` - Fabric-specific operations (sync, mapping, validation)
- `discount_groups_bp` - Discount group management and sync
- `lead_times_bp` - Lead time publishing for pre-Christmas cutoffs
- `excel_tools_bp` - Excel file cleaning and processing utilities

### Service Layer

The `services/` directory contains all business logic, separated from routes:

**Core Services:**
- `database.py` - DatabaseManager class, handles SQLite connections with custom error handling
- `config_service.py` - ConfigManager for loading/accessing config.json
- `job_service.py` - Background job tracking (status, progress, logs stored in database)
- `auth.py` - HTTP Basic Auth decorator for route protection

**External API Clients:**
- `buz_web.py` - BuzClient for Buz Manager REST API (Bearer token auth)
- `odata_client.py` - OData client for Buz Manager's OData feed
- `unleashed_api.py` - UnleashedAPIClient with HMAC-SHA256 signature auth
- `google_sheets_service.py` - GoogleSheetsService wrapping gspread with retry logic

**Data Processing:**
- `excel.py` - Excel file reading/writing with openpyxl
- `data_processing.py` - DataFrame transformations and validations
- `upload.py` - Process uploaded Excel files and sync to database
- `fabric_helpers.py` - Fabric-specific business logic (material restrictions, group mappings)

**Sync Operations:**
- `unleashed_sync.py` - Sync inventory data from Unleashed to local database
- `curtain_fabric_sync.py` - Sync curtain fabrics from Google Sheets to Buz
- `fabric_mapping_sync.py` - Sync fabric group mappings
- `discount_groups_sync.py` - Sync discount groups from Google Sheets to Buz
- `sync_pricing.py` - Generate Buz pricing files from database

**Lead Times Module:**
The `services/lead_times/` package generates pre-Christmas cutoff HTML and Excel files:
- `api.py` - Main orchestration and publishing logic
- `model.py` - Data models (Store, LeadTimeEntry, CutoffEntry)
- `sheets.py` - Fetch data from Google Sheets (Lead Times and Cutoffs tabs)
- `parse.py` - Parse lead time text (e.g., "3-4 weeks") with duplicate resolution
- `html_out.py` - Generate HTML snippets for Buz homepage
- `excel_out.py` - Inject lead times into Excel macro templates
- `links.py` - Build Google Sheets edit links with cell ranges

### Configuration

`config.json` is the central configuration file containing:
- Google Sheets IDs and ranges for various data sources
- Column mappings for Buz inventory and pricing files
- Inventory group codes and material restrictions by group
- Discount group configuration and Google Sheet structure
- Lead times configuration (stores, cutoffs, templates)
- Wastage percentages by product type

Key configuration sections:
- `spreadsheets` - Google Sheets data sources (backorders, unleashed data, curtain fabrics)
- `headers` - Column mappings for Excel exports (buz_inventory_item_file, buz_pricing_file, unleashed_fields)
- `unleashed_group_to_inventory_groups` - Mapping between Unleashed product groups and Buz inventory groups
- `material_restrictions_by_group` - Which materials are allowed for each inventory group
- `discount_grid` - Structure of discount groups Google Sheet
- `lead_times` - Store configurations, cutoff dates, and template paths

### Database Schema

Key tables (see `services/database.py` for `init_db`):
- `jobs` - Background job tracking (id, status, pct, log, error, result, updated_at)
- `buz_inventory` - Cached inventory items from Buz
- `unleashed_inventory` - Cached inventory from Unleashed
- Additional tables for fabrics, mappings, and sync tracking

### Job System

Background jobs are tracked in the database with real-time progress updates:
- `create_job(job_id, db)` - Initialize a new job
- `update_job(job_id, pct, message, error, result, done, db)` - Update job progress
- `get_job(job_id, db)` - Retrieve job status

Jobs have statuses: `running`, `completed`, `failed`, `aborted`

## Key Workflows

### Fabric Sync from Google Sheets to Buz
1. Fetch master curtain fabric list from Google Sheets
2. Validate fabric data (required fields, width, cost/sell prices)
3. Match against existing Buz inventory by supplier product code
4. Generate Excel upload file with ADD/UPDATE/REMOVE operations
5. User downloads and imports into Buz

### Unleashed to Buz Sync
1. Fetch all products from Unleashed API (paginated)
2. Store in local database
3. Match with Buz inventory groups using mapping config
4. Apply material restrictions and validation rules
5. Generate Buz inventory and pricing Excel files

### Discount Groups Sync
1. Fetch discount grid from Google Sheets (multiple customer tabs)
2. Parse product codes and discount percentages
3. Match products against Buz name mapping tab
4. Generate discount group codes for each customer
5. Create Excel file for Buz import

### Lead Times Publishing
1. Fetch lead times and cutoff dates from Google Sheets (separate tabs per store)
2. Parse lead time text (handles ranges like "3-4 weeks")
3. Validate that lead times and cutoffs resolve to same Buz inventory codes
4. Generate HTML snippets for Buz homepage
5. Inject lead times into Excel macro templates (Summary and Detailed)

## Important Patterns

### Database Context
Always use `g.db` for database operations within request handlers. The connection is automatically created in `before_request` and closed in `teardown_request`.

### Configuration Access
Access config via `current_app.config` in routes, or `ConfigManager().config` in services. The merged config contains both Python config classes and `config.json` values.

### Path Management
The app distinguishes between:
- `PROJECT_ROOT` - Repository root directory
- `INSTANCE_ROOT` - Runtime data directory (`instance/` by default)
- `EXPORT_ROOT` - Generic export directory for generated files
- `upload_folder` - User uploads directory
- `UPLOAD_OUTPUT_DIR` - Processed upload outputs

### Error Handling
Services raise custom exceptions (see `services/exceptions.py`). Routes catch these and return appropriate HTTP responses with error messages.

### Excel File Operations
- Use `services/excel.py` for openpyxl operations
- Use `services/excel_safety.py` for validation before processing
- Always validate headers match expected config before processing uploaded files
- Use `keep_vba=True` when working with macro-enabled files

### Google Sheets Integration
- GoogleSheetsService handles authentication and retries
- Service account must have edit access to all sheets
- Use exponential backoff for API rate limiting
- Cache sheet data locally when possible

## Testing Notes

- Tests use in-memory SQLite database (`:memory:`)
- `conftest.py` provides fixtures for app context, database manager, and auth headers
- Mock external APIs (Unleashed, Buz, Google Sheets) in unit tests
- Integration tests in `tests/integration/` may require credentials

## Code Style Notes

- Service functions are generally pure/stateless where possible
- Database connections passed explicitly or via Flask `g` object
- Type hints used in newer code (especially services/buz_web.py, lead_times module)
- Logging configured at app level, use module-level loggers in services
