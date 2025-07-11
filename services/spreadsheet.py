"""Google Sheets integration service for WhatsApp conversation data."""

from typing import Any, Dict, List

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Google Sheets configuration
SPREADSHEET_ID = "1vJItZ03PiZ4Y3HSwBnK_AUCOiQK32OkynMCd-1exU9k"
CREDENTIALS_FILE = "credentials.json"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_sheets_service():
    """Initialize and return Google Sheets API service."""
    try:
        credentials = Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )
        service = build('sheets', 'v4', credentials=credentials)
        return service
    except Exception as e:
        print(f"Error initializing Google Sheets service: {e}")
        return None

def get_sheet_data(sheet_name: str = "report", range_name: str = None) -> List[List]:
    """Read data from Google Sheet using batch get method."""
    service = get_sheets_service()
    if not service:
        return []
    
    try:
        # Use batchGet method which handles special characters better
        result = service.spreadsheets().values().batchGet(
            spreadsheetId=SPREADSHEET_ID,
            ranges=[sheet_name]
        ).execute()
        
        value_ranges = result.get('valueRanges', [])
        if value_ranges:
            return value_ranges[0].get('values', [])
        else:
            return []
            
    except Exception as e:
        print(f"Error reading sheet data from {sheet_name}: {e}")
        return []

def update_sheet_row(row_number: int, values: List[Any], sheet_name: str = "Sheet1") -> bool:
    """Update a specific row in the Google Sheet."""
    service = get_sheets_service()
    if not service:
        return False
    
    try:
        range_name = f"{sheet_name}!A{row_number}:Z{row_number}"
        
        body = {
            'values': [values]
        }
        
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        
        return result.get('updatedCells', 0) > 0
    except Exception as e:
        print(f"Error updating sheet row {row_number}: {e}")
        return False

def update_sheet_cell(row: int, col: str, value: Any, sheet_name: str = "Sheet1") -> bool:
    """Update a specific cell in the Google Sheet."""
    service = get_sheets_service()
    if not service:
        return False
    
    try:
        range_name = f"{sheet_name}!{col}{row}"
        
        body = {
            'values': [[value]]
        }
        
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        
        return result.get('updatedCells', 0) > 0
    except Exception as e:
        print(f"Error updating cell {col}{row}: {e}")
        return False

def update_sheet(changes: List[Dict[str, Any]]) -> List[int]:
    """
    Update multiple records in Google Sheet.
    changes: [{id: int, field: str, new_value: Any, row_number: int, column: str}, …]
    Returns list of IDs successfully updated.
    """
    successful_updates = []
    
    for change in changes:
        record_id = change.get('id')
        field = change.get('field')
        new_value = change.get('new_value')
        row_number = change.get('row_number')
        column = change.get('column')
        sheet_name = change.get('sheet_name', 'Sheet1')
        
        if row_number and column:
            success = update_sheet_cell(row_number, column, new_value, sheet_name)
            if success:
                successful_updates.append(record_id)
                print(f"Updated {field} for ID {record_id} at {column}{row_number}")
            else:
                print(f"Failed to update {field} for ID {record_id}")
    
    return successful_updates

def sync_record_to_sheet(record_data: Dict[str, Any], whatsapp_number: str, sheet_name: str = "Sheet1") -> bool:
    """Sync a complete record to Google Sheet by finding the row with matching WhatsApp number."""
    service = get_sheets_service()
    if not service:
        return False
    
    try:
        # First, get all data to find the matching row
        sheet_data = get_sheet_data(sheet_name)
        if not sheet_data:
            return False
        
        # Find the header row (assume first row)
        headers = sheet_data[0] if sheet_data else []
        
        # Find the row with matching WhatsApp number
        target_row = None
        whatsapp_col_index = None
        
        # Find WhatsApp column (prioritize 'celular')
        search_terms = ['celular', 'whatsapp', 'phone', 'numero', 'contato', 'telefone']
        for i, header in enumerate(headers):
            header_lower = str(header).lower()
            for term in search_terms:
                if term in header_lower:
                    whatsapp_col_index = i
                    break
            if whatsapp_col_index is not None:
                break
        
        if whatsapp_col_index is None:
            print("WhatsApp column not found in sheet")
            return False
        
        # Clean phone numbers for matching - Enhanced algorithm
        def clean_phone_for_match(phone):
            if not phone:
                return ""
            
            # Clean whitespace and special characters
            import re
            clean = re.sub(r'[\s\t\n\r]', '', str(phone))
            clean = re.sub(r'[^0-9]', '', clean)
            
            # Handle edge cases where phone might be too short
            if len(clean) < 8:
                return ""
            
            # Remove country code if present (55 for Brazil)
            if clean.startswith('55') and len(clean) > 10:
                clean = clean[2:]
            
            # Brazilian area codes
            valid_area_codes = ['11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55', '61', '62', '63', '64', '65', '66', '67', '68', '69', '71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '91', '92', '93', '94', '95', '96', '97', '98', '99']
            
            # If 10 digits and starts with valid area code, add mobile prefix
            if len(clean) == 10 and clean[:2] in valid_area_codes:
                area_code = clean[:2]
                number = clean[2:]
                # If it's a mobile number (starts with 6, 7, 8, or 9) and doesn't have 9 prefix
                if number[0] in '6789' and not number.startswith('9'):
                    clean = area_code + '9' + number
            
            return clean
        
        # Enhanced matching function that tries multiple variants
        def find_phone_match(target_phone, sheet_data, whatsapp_col_index):
            clean_target = clean_phone_for_match(target_phone)
            
            # Brazilian area codes
            valid_area_codes = ['11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55', '61', '62', '63', '64', '65', '66', '67', '68', '69', '71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '91', '92', '93', '94', '95', '96', '97', '98', '99']
            
            # Create variants to try
            variants = [clean_target]
            
            # If target is 10 digits with area code, try adding mobile prefix
            if len(clean_target) == 10 and clean_target[:2] in valid_area_codes:
                area_code = clean_target[:2]
                number = clean_target[2:]
                # Add mobile prefix for numbers that start with 6, 7, 8, or 9
                if number[0] in '6789':
                    variants.append(area_code + '9' + number)
            
            # If target has mobile prefix, also try without it
            if len(clean_target) == 11 and clean_target[:2] in valid_area_codes:
                area_code = clean_target[:2]
                mobile_prefix = clean_target[2]
                number = clean_target[3:]
                if mobile_prefix == '9' and number[0] in '6789':
                    variants.append(area_code + number)
            
            print(f"Trying phone variants: {variants}")
            
            # Try to find a match with any variant
            for variant in variants:
                for i, row in enumerate(sheet_data[1:], start=2):
                    if whatsapp_col_index < len(row):
                        sheet_phone = clean_phone_for_match(row[whatsapp_col_index])
                        if sheet_phone and sheet_phone == variant:
                            print(f"Found match for variant '{variant}' at row {i}")
                            return i
            
            return None
        
        # Find matching row using enhanced matching
        target_phone = whatsapp_number.split('@')[0] if '@' in whatsapp_number else whatsapp_number
        target_row = find_phone_match(target_phone, sheet_data, whatsapp_col_index)
        
        if target_row is None:
            print(f"Row with WhatsApp number {whatsapp_number} not found")
            return False
        
        # Update the fields that exist in both record_data and headers
        updated_fields = []
        for field_name, value in record_data.items():
            # Find column for this field
            col_index = None
            for i, header in enumerate(headers):
                if header.lower() == field_name.lower():
                    col_index = i
                    break
            
            if col_index is not None:
                # Convert column index to letter (A, B, C, etc.)
                col_letter = chr(65 + col_index)  # A=65 in ASCII
                
                # Update the cell
                success = update_sheet_cell(target_row, col_letter, value, sheet_name)
                if success:
                    updated_fields.append(field_name)
        
        print(f"Updated {len(updated_fields)} fields for WhatsApp {whatsapp_number}: {updated_fields}")
        return len(updated_fields) > 0
        
    except Exception as e:
        print(f"Error syncing record to sheet: {e}")
        return False
