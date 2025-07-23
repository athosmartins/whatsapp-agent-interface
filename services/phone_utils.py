"""
Centralized phone number utilities for consistent phone handling across the application.

This module provides unified functions to handle the different phone number formats:
- Database format: 553191156109 (no +, no 9th digit) - 12 digits
- Spreadsheet format: +5531991156109 (complete: + + 55 + 31 + 9 + 91156109) - 14 chars
- Display format: (31) 99115-6109 (Brazilian standard)

Phone number structure:
- + : International prefix
- 55: Brazil country code  
- 31: Area code (2 digits)
- 9 : Mobile prefix (added in 2012)
- 91156109: 8-digit phone number
"""

import re
import pandas as pd
from typing import Optional, List


# Brazilian area codes for validation
VALID_AREA_CODES = [
    '11', '12', '13', '14', '15', '16', '17', '18', '19',  # São Paulo state
    '21', '22', '24', '27', '28',                         # Rio de Janeiro state  
    '31', '32', '33', '34', '35', '37', '38',            # Minas Gerais state
    '41', '42', '43', '44', '45', '46', '47', '48', '49', # Paraná/Santa Catarina
    '51', '53', '54', '55',                              # Rio Grande do Sul
    '61', '62', '63', '64', '65', '66', '67', '68', '69', # Centro-Oeste
    '71', '73', '74', '75', '77', '79',                  # Bahia/Sergipe
    '81', '82', '83', '84', '85', '86', '87', '88', '89', # Nordeste
    '91', '92', '93', '94', '95', '96', '97', '98', '99'  # Norte
]


def normalize_db_to_spreadsheet(phone: str) -> str:
    """
    Convert database format to spreadsheet format.
    
    Database format: 553191156109 (12 digits, no +, no 9th digit)
    Spreadsheet format: +5531991156109 (14 chars, complete format)
    
    Args:
        phone: Phone in database format (553191156109)
        
    Returns:
        Phone in spreadsheet format (+5531991156109)
        
    Examples:
        >>> normalize_db_to_spreadsheet('553191156109')
        '+5531991156109'
        >>> normalize_db_to_spreadsheet('551187654321') 
        '+5511987654321'
    """
    if not phone or pd.isna(phone):
        return ""
    
    # Clean input - remove all non-digits
    clean = re.sub(r'[^0-9]', '', str(phone))
    
    # Remove @domain if present (WhatsApp format)
    if '@' in str(phone):
        clean = str(phone).split('@')[0]
        clean = re.sub(r'[^0-9]', '', clean)
    
    # Validate DB format: exactly 12 digits
    if len(clean) != 12:
        # If not exact DB format, return as-is
        return phone
    
    # Extract components: 55 + area(2) + phone(8) -> +55 + area(2) + 9 + phone(8)
    country = clean[:2]  # Should be '55'
    area_code = clean[2:4]  # Next 2 digits
    phone_number = clean[4:]  # Remaining 8 digits
    
    # Validate components
    if country != '55':
        return phone  # Not Brazilian number
        
    if area_code not in VALID_AREA_CODES:
        return phone  # Invalid area code
        
    if len(phone_number) != 8:
        return phone  # Invalid phone length
    
    # Build complete format: +55 + area + 9 + phone
    result = f"+55{area_code}9{phone_number}"
    
    return result


def normalize_spreadsheet_to_db(phone: str) -> str:
    """
    Convert spreadsheet format to database format.
    
    Spreadsheet format: +5531991156109 (14 chars, complete format)
    Database format: 553191156109 (12 digits, no +, no 9th digit)
    
    Args:
        phone: Phone in spreadsheet format (+5531991156109)
        
    Returns:
        Phone in database format (553191156109)
        
    Examples:
        >>> normalize_spreadsheet_to_db('+5531991156109')
        '553191156109'
        >>> normalize_spreadsheet_to_db('+5511987654321')
        '551187654321'
    """
    if not phone or pd.isna(phone):
        return ""
    
    # Clean input - remove all non-digits
    clean = re.sub(r'[^0-9]', '', str(phone))
    
    # Validate spreadsheet format: should be 13 digits after cleaning +55
    if len(clean) != 13:
        return phone  # Not expected format
    
    # Extract components: 55 + area(2) + 9 + phone(8) -> 55 + area(2) + phone(8)
    country = clean[:2]  # Should be '55'
    area_code = clean[2:4]  # Next 2 digits  
    mobile_prefix = clean[4]  # Should be '9'
    phone_number = clean[5:]  # Remaining 8 digits
    
    # Validate components
    if country != '55':
        return phone  # Not Brazilian number
        
    if area_code not in VALID_AREA_CODES:
        return phone  # Invalid area code
        
    if mobile_prefix != '9':
        return phone  # Not mobile number
        
    if len(phone_number) != 8:
        return phone  # Invalid phone length
    
    # Build DB format: 55 + area + phone (remove mobile prefix 9)
    result = f"55{area_code}{phone_number}"
    
    return result


def clean_phone_for_matching(phone: str) -> str:
    """
    Universal phone cleaning for matching between different formats.
    
    This function normalizes phones to a consistent format for comparison:
    - DB format (553191156109) -> 5531991156109 (add mobile 9)
    - Spreadsheet format (+5531991156109) -> 5531991156109 (remove +)
    - Other formats -> best effort normalization
    
    Args:
        phone: Phone in any format
        
    Returns:
        Normalized phone for matching (5531991156109)
        
    Examples:
        >>> clean_phone_for_matching('553191156109')  # DB format
        '5531991156109'
        >>> clean_phone_for_matching('+5531991156109')  # Spreadsheet format  
        '5531991156109'
    """
    if not phone or pd.isna(phone):
        return ""
    
    # Clean input - remove all non-digits and whitespace
    clean = re.sub(r'[\s\t\n\r]', '', str(phone))
    clean = re.sub(r'[^0-9]', '', clean)
    
    # Remove @domain if present (WhatsApp format)
    if '@' in str(phone):
        clean = str(phone).split('@')[0]
        clean = re.sub(r'[^0-9]', '', clean)
    
    # Handle edge cases
    if len(clean) < 8:
        return ""
    
    # Case 1: DB format (12 digits: 55 + area + 8-digit phone)
    if len(clean) == 12 and clean.startswith('55'):
        # Convert to spreadsheet format first, then normalize
        spreadsheet_format = normalize_db_to_spreadsheet(clean)
        if spreadsheet_format.startswith('+'):
            return re.sub(r'[^0-9]', '', spreadsheet_format)
        return clean
    
    # Case 2: Spreadsheet format (13 digits after removing +: 55 + area + 9 + phone)
    if len(clean) == 13 and clean.startswith('55'):
        country = clean[:2]
        area_code = clean[2:4]
        mobile_prefix = clean[4]
        phone_number = clean[5:]
        
        # Validate format
        if (country == '55' and 
            area_code in VALID_AREA_CODES and 
            mobile_prefix == '9' and 
            len(phone_number) == 8):
            return clean  # Already in correct format
    
    # Case 3: Remove country code if present and length > 10
    if clean.startswith('55') and len(clean) > 10:
        # Only remove if it results in valid Brazilian mobile format
        without_country = clean[2:]
        if len(without_country) == 11:  # area(2) + 9 + phone(8)
            area_code = without_country[:2]
            if area_code in VALID_AREA_CODES:
                return clean  # Keep full format
    
    # Case 4: Add mobile prefix if missing (10 digits: area + phone without 9)
    if len(clean) == 10:
        area_code = clean[:2]
        phone_number = clean[2:]
        if (area_code in VALID_AREA_CODES and 
            phone_number[0] in '6789'):  # Likely mobile number
            # Add country code and mobile prefix
            return f"55{area_code}9{phone_number}"
    
    # Case 5: Already has mobile prefix (11 digits: area + 9 + phone)
    if len(clean) == 11:
        area_code = clean[:2]
        mobile_prefix = clean[2]
        if (area_code in VALID_AREA_CODES and mobile_prefix == '9'):
            # Add country code
            return f"55{clean}"
    
    # Return as-is if no pattern matches
    return clean


def format_phone_for_storage(phone: str) -> str:
    """
    Format phone number for storage in Google Sheets.
    
    Always outputs complete format with apostrophe prefix to prevent 
    Google Sheets from interpreting as formula.
    
    Args:
        phone: Phone in any format
        
    Returns:
        Phone formatted for storage ('+5531991156109)
        
    Examples:
        >>> format_phone_for_storage('553191156109')
        "'+5531991156109"
        >>> format_phone_for_storage('+5531991156109')
        "'+5531991156109"
    """
    if not phone or pd.isna(phone):
        return ""
    
    # First normalize to get consistent format
    normalized = clean_phone_for_matching(phone)
    if not normalized:
        return phone  # Return original if normalization failed
    
    # Convert to spreadsheet format
    if len(normalized) == 13 and normalized.startswith('55'):
        # Already in correct format, add apostrophe and +
        return f"'+{normalized}"
    
    # Try to convert from other formats
    if len(normalized) == 12:
        # Might be DB format
        converted = normalize_db_to_spreadsheet(normalized)
        if converted.startswith('+'):
            return f"'{converted}"
    
    # Default: ensure it starts with +55 and has apostrophe
    clean = re.sub(r'[^0-9]', '', str(phone))
    if len(clean) >= 11:
        # Take last 11 digits and add +55 prefix
        area_and_phone = clean[-11:]
        return f"'+55{area_and_phone}"
    
    return phone  # Return original if can't format


def format_phone_for_display(phone: str) -> str:
    """
    Format phone number for display in Brazilian standard format.
    
    Always outputs (XX) XXXXX-XXXX format for consistent UI display.
    
    Args:
        phone: Phone in any format
        
    Returns:
        Phone formatted for display ((31) 99115-6109)
        
    Examples:
        >>> format_phone_for_display('553191156109')
        '(31) 99115-6109'
        >>> format_phone_for_display('+5531991156109') 
        '(31) 99115-6109'
    """
    if not phone or pd.isna(phone):
        return ""
    
    # First normalize to get consistent format
    normalized = clean_phone_for_matching(phone)
    if not normalized:
        return phone  # Return original if normalization failed
    
    # Extract area code and phone number from normalized format
    if len(normalized) == 13 and normalized.startswith('55'):
        # Format: 55 + area(2) + 9 + phone(8)
        area_code = normalized[2:4]
        mobile_prefix = normalized[4]
        phone_number = normalized[5:]
        
        if len(phone_number) == 8:
            # Format as (XX) 9XXXX-XXXX
            return f"({area_code}) {mobile_prefix}{phone_number[:4]}-{phone_number[4:]}"
    
    # Fallback: try to extract from raw phone
    clean = re.sub(r'[^0-9]', '', str(phone))
    
    if len(clean) >= 10:
        # Take last 10-11 digits and format
        if len(clean) == 11:  # area + 9 + phone
            area = clean[:2]
            mobile = clean[2]
            number = clean[3:]
            if len(number) == 8:
                return f"({area}) {mobile}{number[:4]}-{number[4:]}"
        elif len(clean) == 10:  # area + phone (no mobile prefix)
            area = clean[:2]
            number = clean[2:]
            if len(number) == 8:
                return f"({area}) {number[:4]}-{number[4:]}"
    
    return phone  # Return original if can't format


def generate_phone_variants(phone: str) -> List[str]:
    """
    Generate all possible variants of a phone number for matching.
    
    This function creates multiple representations of the same phone number
    to handle legacy data and different input formats.
    
    Args:
        phone: Phone in any format
        
    Returns:
        List of phone variants for matching
        
    Examples:
        >>> generate_phone_variants('553191156109')
        ['5531991156109', '553191156109', '+5531991156109']
    """
    if not phone or pd.isna(phone):
        return []
    
    variants = set()
    base_clean = clean_phone_for_matching(phone)
    
    if base_clean:
        variants.add(base_clean)
        
        # Add DB format variant if normalized format is different
        if len(base_clean) == 13:
            db_format = normalize_spreadsheet_to_db(f"+{base_clean}")
            if db_format and db_format != base_clean:
                variants.add(db_format)
        
        # Add spreadsheet format variant
        if len(base_clean) == 12:
            spreadsheet_format = normalize_db_to_spreadsheet(base_clean)
            if spreadsheet_format.startswith('+'):
                variants.add(re.sub(r'[^0-9]', '', spreadsheet_format))
        
        # Add format without country code if present
        if base_clean.startswith('55') and len(base_clean) > 11:
            without_country = base_clean[2:]
            if len(without_country) == 11:  # Valid mobile format
                variants.add(without_country)
        
        # Add format with country code if missing
        if not base_clean.startswith('55') and len(base_clean) == 11:
            with_country = f"55{base_clean}"
            variants.add(with_country)
    
    return sorted(list(variants))


def is_valid_brazilian_phone(phone: str) -> bool:
    """
    Check if a phone number is a valid Brazilian mobile number.
    
    Args:
        phone: Phone number to validate
        
    Returns:
        True if valid Brazilian mobile number, False otherwise
        
    Examples:
        >>> is_valid_brazilian_phone('553191156109')
        True
        >>> is_valid_brazilian_phone('+5531991156109')
        True
        >>> is_valid_brazilian_phone('123456')
        False
    """
    if not phone or pd.isna(phone):
        return False
    
    normalized = clean_phone_for_matching(phone)
    
    # Check if normalized format is valid
    if len(normalized) == 13 and normalized.startswith('55'):
        area_code = normalized[2:4]
        mobile_prefix = normalized[4]
        phone_number = normalized[5:]
        
        return (area_code in VALID_AREA_CODES and 
                mobile_prefix == '9' and 
                len(phone_number) == 8 and
                phone_number[0] in '6789')  # Mobile numbers start with 6-9
    
    return False


# Utility function for debugging
def debug_phone_conversion(phone: str) -> dict:
    """
    Debug function to show all phone conversion steps.
    
    Args:
        phone: Phone number to debug
        
    Returns:
        Dictionary with conversion details
    """
    return {
        'original': phone,
        'normalized_for_matching': clean_phone_for_matching(phone),
        'db_format': normalize_spreadsheet_to_db(f"+{clean_phone_for_matching(phone)}") if clean_phone_for_matching(phone) else "",
        'spreadsheet_format': normalize_db_to_spreadsheet(phone) if len(re.sub(r'[^0-9]', '', str(phone))) == 12 else "",
        'storage_format': format_phone_for_storage(phone),
        'display_format': format_phone_for_display(phone),
        'variants': generate_phone_variants(phone),
        'is_valid': is_valid_brazilian_phone(phone)
    }