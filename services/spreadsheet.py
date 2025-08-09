"""Google Sheets integration service for WhatsApp conversation data."""

from typing import Any, Dict, List
import streamlit as st

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Import centralized phone utilities
from services.phone_utils import (
    clean_phone_for_matching,
    format_phone_for_storage as format_phone_for_storage_new,
    generate_phone_variants
)

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
        import traceback
        print("Full service initialization error traceback:")
        traceback.print_exc()
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
    # Use centralized phone utility
    formatted = format_phone_for_storage_new(phone)
    if formatted and formatted != phone:
        print(f"📱 Phone formatted: {phone} → {formatted}")
    return formatted

def clean_phone_for_match(phone):
    """Clean phone numbers for matching - Enhanced algorithm."""
    # Use centralized phone utility for consistent behavior
    return clean_phone_for_matching(phone)

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
    
    # Use centralized phone utilities to generate variants
    variants = generate_phone_variants(target_phone)
    
    # Try to find a match with any variant
    for variant in variants:
        for i, row in enumerate(sheet_data[1:], start=1):
            if whatsapp_col_index < len(row):
                sheet_phone = clean_phone_for_matching(row[whatsapp_col_index])
                if sheet_phone and sheet_phone == variant:
                    return (row, i)
    
    return None

def get_sheet_data_direct(sheet_name: str = "report", range_name: str = None) -> List[List]:
    """Read data from Google Sheet using batch get method - DIRECT ACCESS (bypasses session controls)."""
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
        import traceback
        print("Full Google Sheets error traceback:")
        traceback.print_exc()
        return []

@st.cache_data(ttl=1800)  # Cache for 30 minutes - RESTORED ORIGINAL LOGIC
def get_sheet_data(sheet_name: str = "report", range_name: str = None, force_load: bool = False) -> List[List]:
    """
    Read data from Google Sheet - RESTORED TO ORIGINAL WORKING LOGIC.
    Uses simple Streamlit caching instead of complex session state management.
    
    Args:
        sheet_name: Name of the sheet tab to read from
        range_name: Optional range specification (unused in current implementation)  
        force_load: If True, clears cache and forces fresh load from spreadsheet
    
    Returns:
        List of lists containing spreadsheet data
    """
    if force_load:
        # Clear the cache and force fresh load
        get_sheet_data.clear()
        print(f"🔄 Force loading spreadsheet data from {sheet_name}")
    
    # Load fresh data from spreadsheet (will be cached by Streamlit)
    sheet_data = get_sheet_data_direct(sheet_name, range_name)
    print(f"📋 Loaded spreadsheet data: {len(sheet_data)} rows")
    return sheet_data

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

def sync_record_to_sheet(record_data: Dict[str, Any], whatsapp_number: str, sheet_name: str = "Sheet1", essential_fields: Dict[str, Any] = None, partial_update: bool = True) -> Dict[str, Any]:
    """
    Sync record to Google Sheet with support for partial updates.
    
    Args:
        record_data: Fields to update in the spreadsheet
        whatsapp_number: Phone number to identify the row
        sheet_name: Name of the sheet to update
        essential_fields: Required fields for new row creation (cpf, Nome, etc.)
        partial_update: If True, only update specified fields; if False, update entire row
    """
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
        
        # Enhanced matching function using centralized utilities
        def find_phone_match_local(target_phone, sheet_data, whatsapp_col_index):
            variants = generate_phone_variants(target_phone)
            print(f"Trying phone variants: {variants}")
            
            # Try to find a match with any variant
            for variant in variants:
                for i, row in enumerate(sheet_data[1:], start=2):
                    if whatsapp_col_index < len(row):
                        sheet_phone = clean_phone_for_matching(row[whatsapp_col_index])
                        if sheet_phone and sheet_phone == variant:
                            print(f"Found match for variant '{variant}' at row {i}")
                            return i
            
            return None
        
        # Find matching row using enhanced matching
        target_phone = whatsapp_number.split('@')[0] if '@' in whatsapp_number else whatsapp_number
        target_row = find_phone_match_local(target_phone, sheet_data, whatsapp_col_index)
        
        if target_row is None:
            print(f"Row with WhatsApp number {whatsapp_number} not found - creating new row")
            # For new row creation, merge record_data with essential_fields
            new_row_data = {}
            if essential_fields:
                new_row_data.update(essential_fields)
            new_row_data.update(record_data)  # record_data takes precedence
            return create_new_row_in_sheet(new_row_data, whatsapp_number, sheet_name)
        
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
        if len(updated_fields) > 0:
            return {
                "success": True,
                "action": "updated",
                "row_number": target_row,
                "updated_fields": updated_fields,
                "field_mappings": {field: record_data[field] for field in updated_fields},
                "message": f"Updated {len(updated_fields)} fields in row {target_row}"
            }
        else:
            # No fields were updated - values are already identical
            return {
                "success": True,
                "action": "already_synced",
                "row_number": target_row,
                "updated_fields": [],
                "field_mappings": {},
                "message": f"Spreadsheet already has identical values (row {target_row})"
            }
        
    except Exception as e:
        print(f"Error syncing record to sheet: {e}")
        return {"success": False, "error": str(e), "action": "failed"}
