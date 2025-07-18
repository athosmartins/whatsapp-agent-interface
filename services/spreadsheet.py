"""Google Sheets integration service for WhatsApp conversation data."""

from typing import Any, Dict, List
import streamlit as st

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Google Sheets configuration
SPREADSHEET_ID = "1vJItZ03PiZ4Y3HSwBnK_AUCOiQK32OkynMCd-1exU9k"
CREDENTIALS_FILE = "credentials.json"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_sheets_service():
    """Initialize and return Google Sheets API service."""
    try:
        # Try to load from Streamlit secrets first (for production)
        credentials_info = None
        
        try:
            import streamlit as st
            if hasattr(st, 'secrets') and 'google_sheets' in st.secrets:
                credentials_info = dict(st.secrets['google_sheets'])
                credentials = Credentials.from_service_account_info(
                    credentials_info, scopes=SCOPES
                )
                print("Using Google Sheets credentials from Streamlit secrets")
            else:
                raise Exception("Streamlit secrets not available")
        except:
            # Fallback to local credentials.json file (for development)
            import os
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"Error: {CREDENTIALS_FILE} not found and Streamlit secrets not configured.")
                return None
            
            credentials = Credentials.from_service_account_file(
                CREDENTIALS_FILE, scopes=SCOPES
            )
            print("Using Google Sheets credentials from local file")
        
        service = build('sheets', 'v4', credentials=credentials)
        return service
    except Exception as e:
        print(f"Error initializing Google Sheets service: {e}")
        return None

def format_address_field(address):
    """Format address field with proper title case."""
    if not address:
        return ""
    
    # Convert to title case while preserving certain abbreviations
    address_str = str(address).strip()
    if not address_str:
        return ""
    
    # Handle common Brazilian address abbreviations
    abbreviations = {
        'rua': 'Rua',
        'av': 'Av',
        'avenida': 'Avenida',
        'r': 'R',
        'travessa': 'Travessa',
        'praca': 'Praça',
        'largo': 'Largo',
        'vila': 'Vila',
        'bairro': 'Bairro',
        'centro': 'Centro',
        'setor': 'Setor',
        'quadra': 'Quadra',
        'lote': 'Lote',
        'apt': 'Apt',
        'apartamento': 'Apartamento',
        'bl': 'Bl',
        'bloco': 'Bloco',
        'ed': 'Ed',
        'edificio': 'Edifício',
        'cond': 'Cond',
        'condominio': 'Condomínio',
        'casa': 'Casa',
        'sobrado': 'Sobrado',
        'sala': 'Sala',
        'loja': 'Loja',
        'andar': 'Andar',
        'subsolo': 'Subsolo',
        'terreo': 'Térreo',
        'cobertura': 'Cobertura'
    }
    
    # Split into words and apply title case with abbreviation handling
    words = address_str.lower().split()
    formatted_words = []
    
    for word in words:
        # Remove common punctuation for lookup
        clean_word = word.strip('.,;:')
        if clean_word in abbreviations:
            formatted_words.append(abbreviations[clean_word])
        else:
            formatted_words.append(clean_word.capitalize())
    
    return ' '.join(formatted_words)

def format_phone_for_storage(phone):
    """Format phone number for storage in spreadsheet as '+55 + area + 9 + 8 digits."""
    if not phone:
        return ""
    
    # Clean the phone number
    import re
    clean = re.sub(r'[^0-9]', '', str(phone))
    
    # Remove country code if present
    if clean.startswith('55') and len(clean) > 10:
        clean = clean[2:]
    
    # Brazilian area codes
    valid_area_codes = ['11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55', '61', '62', '63', '64', '65', '66', '67', '68', '69', '71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '91', '92', '93', '94', '95', '96', '97', '98', '99']
    
    # Validate and format
    if len(clean) == 10 and clean[:2] in valid_area_codes:
        # 10 digits: add mobile prefix if missing
        area_code = clean[:2]
        number = clean[2:]
        if number[0] in '6789':
            clean = area_code + '9' + number
    elif len(clean) == 11 and clean[:2] in valid_area_codes:
        # 11 digits: already has mobile prefix
        pass
    else:
        # Invalid format, return as is
        return phone
    
    # Format as '+55 + area + 9 + 8 digits (apostrophe prevents Google Sheets formula interpretation)
    if len(clean) == 11:
        formatted = f"'+55{clean}"
        print(f"📱 Phone formatted: {phone} → {formatted}")
        return formatted
    else:
        print(f"📱 Phone format invalid, returning as-is: {phone} (cleaned: {clean})")
        return phone

def clean_phone_for_match(phone):
    """Clean phone numbers for matching - Enhanced algorithm."""
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

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def find_phone_match(target_phone, sheet_data):
    """Enhanced matching function that tries multiple variants."""
    if not sheet_data or len(sheet_data) < 2:
        return None
        
    # Find WhatsApp column
    headers = sheet_data[0]
    whatsapp_col_index = None
    for i, header in enumerate(headers):
        if 'celular' in str(header).lower() or 'whatsapp' in str(header).lower() or 'phone' in str(header).lower():
            whatsapp_col_index = i
            break
    
    if whatsapp_col_index is None:
        return None
    
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
    
    # Try to find a match with any variant
    for variant in variants:
        for i, row in enumerate(sheet_data[1:], start=1):
            if whatsapp_col_index < len(row):
                sheet_phone = clean_phone_for_match(row[whatsapp_col_index])
                if sheet_phone and sheet_phone == variant:
                    return (row, i)
    
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

def update_sheet_cell_with_service(service, row: int, col: str, value: Any, sheet_name: str = "Sheet1") -> bool:
    """Update a specific cell in the Google Sheet using existing service (avoids re-authentication)."""
    if not service:
        print(f"🔍 **API Debug:** Google Sheets service not available")
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
        
        updated_cells = result.get('updatedCells', 0)
        return updated_cells > 0
    except Exception as e:
        print(f"🔍 **API Error:** Error updating cell {col}{row}: {e}")
        import traceback
        print(f"🔍 **API Error Details:** {traceback.format_exc()}")
        return False

def create_new_row_in_sheet(record_data: Dict[str, Any], whatsapp_number: str, sheet_name: str = "Sheet1") -> Dict[str, Any]:
    """Create a new row in the Google Sheet with proper default values."""
    service = get_sheets_service()
    if not service:
        return {"success": False, "error": "Google Sheets service not available"}
    
    try:
        # Get current sheet data to understand structure
        sheet_data = get_sheet_data(sheet_name)
        if not sheet_data:
            return {"success": False, "error": "Could not read sheet data"}
        
        headers = sheet_data[0] if sheet_data else []
        if not headers:
            return {"success": False, "error": "No headers found in sheet"}
        
        # Calculate the row number for the new row (after last data row)
        current_rows = len(sheet_data)
        new_row_number = current_rows + 1
        
        # The append API should automatically handle row expansion
        print(f"📊 Attempting to create new row {new_row_number} (current sheet has {current_rows} rows)")
        
        # Create row data with proper defaults
        row_values = []
        for header in headers:
            header_lower = str(header).lower().strip()
            
            # Map the record data to the appropriate column
            if header_lower == "celular":
                # Extract just the phone number from WhatsApp ID (remove @s.whatsapp.net)
                clean_phone = whatsapp_number.split('@')[0] if '@' in whatsapp_number else whatsapp_number
                row_values.append(format_phone_for_storage(clean_phone))
            elif header_lower == "nome":
                row_values.append(record_data.get("Nome", ""))
            elif header_lower == "nome_whatsapp":
                row_values.append(record_data.get("nome_whatsapp", ""))
            elif header_lower == "cpf":
                row_values.append(record_data.get("cpf", ""))
            elif header_lower in ["endereco", "endereco_bairro", "endereco_complemento"]:
                # Format address fields with proper title case
                address_value = record_data.get(header, "")
                row_values.append(format_address_field(address_value))
            elif header in record_data:
                row_values.append(record_data[header])
            else:
                # Default values for unmapped fields
                if any(keyword in header_lower for keyword in ["stakeholder", "intermediador", "imovel_anunciado", "imovel_em_inventario", "standby"]):
                    row_values.append("FALSE")
                else:
                    row_values.append("")  # Empty string for all other fields
        
        # Insert the new row using append (more reliable than calculating exact row)
        range_name = f"{sheet_name}!A:Z"
        body = {
            'values': [row_values]
        }
        
        result = service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        
        updated_cells = result.get('updatedCells', 0)
        if updated_cells > 0:
            # Get the actual row number from the append result
            updated_range = result.get('updates', {}).get('updatedRange', '')
            actual_row_number = new_row_number  # fallback to calculated value
            
            # Try to extract row number from the response
            if updated_range:
                import re
                match = re.search(r'(\d+):\w+(\d+)', updated_range)
                if match:
                    actual_row_number = int(match.group(1))
            
            return {
                "success": True,
                "action": "created",
                "row_number": actual_row_number,
                "updated_cells": updated_cells,
                "field_mappings": {header: row_values[i] for i, header in enumerate(headers) if i < len(row_values) and row_values[i]},
                "message": f"Created new row {actual_row_number} with {updated_cells} cells"
            }
        else:
            return {"success": False, "error": "No cells were updated"}
            
    except Exception as e:
        return {"success": False, "error": f"Error creating new row: {e}"}

def update_sheet_cell(row: int, col: str, value: Any, sheet_name: str = "Sheet1") -> bool:
    """Update a specific cell in the Google Sheet."""
    service = get_sheets_service()
    return update_sheet_cell_with_service(service, row, col, value, sheet_name)

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

def sync_record_to_sheet(record_data: Dict[str, Any], whatsapp_number: str, sheet_name: str = "Sheet1") -> Dict[str, Any]:
    """Sync a complete record to Google Sheet by finding the row with matching WhatsApp number or creating new row."""
    service = get_sheets_service()
    if not service:
        return {"success": False, "error": "Google Sheets service not available", "action": "failed"}
    
    try:
        # First, get all data to find the matching row
        sheet_data = get_sheet_data(sheet_name)
        if not sheet_data:
            return {"success": False, "error": "Could not read sheet data", "action": "failed"}
        
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
            return {"success": False, "error": "WhatsApp column not found in sheet", "action": "failed"}
        
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
            print(f"Row with WhatsApp number {whatsapp_number} not found - creating new row")
            # Create new row instead of failing
            return create_new_row_in_sheet(record_data, whatsapp_number, sheet_name)
        
        # Update the fields that exist in both record_data and headers
        updated_fields = []
        
        for field_name, value in record_data.items():
            # Find column for this field
            col_index = None
            matched_header = None
            for i, header in enumerate(headers):
                # Normalize both strings for comparison - handle special characters
                header_normalized = str(header).lower().strip()
                field_normalized = str(field_name).lower().strip()
                
                # Try exact match first
                if header_normalized == field_normalized:
                    col_index = i
                    matched_header = header
                    break
                
                # Try alternative matching for known problematic fields
                if field_name == "Classificação do dono do número":
                    if "classificacao" in header_normalized or "classificação" in header_normalized:
                        col_index = i
                        matched_header = header
                        break
                elif field_name == "status_manual":
                    if header_normalized == "status_manual":
                        col_index = i
                        matched_header = header
                        break
            
            if col_index is not None:
                # Convert column index to letter (A, B, C, etc.)
                col_letter = chr(65 + col_index)  # A=65 in ASCII
                
                # Update the cell using the existing service (avoid re-authentication)
                success = update_sheet_cell_with_service(service, target_row, col_letter, value, sheet_name)
                
                if success:
                    updated_fields.append(field_name)
                
                # Log key field updates for debugging
                if field_name in ["Classificação do dono do número", "status_manual"]:
                    print(f"🔍 **Key Field Update:** '{field_name}' → {col_letter}{target_row} = {repr(value)} | {'✅ SUCCESS' if success else '❌ FAILED'}")
        
        print(f"Updated {len(updated_fields)} fields for WhatsApp {whatsapp_number}: {updated_fields}")
        
        # Return detailed results
        return {
            "success": len(updated_fields) > 0,
            "action": "updated",
            "row_number": target_row,
            "updated_fields": updated_fields,
            "field_mappings": {field: record_data[field] for field in updated_fields},
            "message": f"Updated {len(updated_fields)} fields in row {target_row}"
        }
        
    except Exception as e:
        print(f"Error syncing record to sheet: {e}")
        return {"success": False, "error": str(e), "action": "failed"}
