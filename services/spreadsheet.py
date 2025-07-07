import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from typing import List, Dict, Any
import pandas as pd

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

def get_sheet_data(sheet_name: str = "Sheet1", range_name: str = None) -> List[List]:
    """Read data from Google Sheet."""
    service = get_sheets_service()
    if not service:
        return []
    
    try:
        if range_name:
            range_to_read = f"{sheet_name}!{range_name}"
        else:
            range_to_read = sheet_name
            
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_to_read
        ).execute()
        
        return result.get('values', [])
    except Exception as e:
        print(f"Error reading sheet data: {e}")
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
            valueInputOption='RAW',
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
            valueInputOption='RAW',
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
        
        # Find matching row
        for i, row in enumerate(sheet_data[1:], start=2):  # Start from row 2 (after header)
            if i <= len(row) and whatsapp_col_index < len(row):
                if row[whatsapp_col_index] == whatsapp_number:
                    target_row = i
                    break
        
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
