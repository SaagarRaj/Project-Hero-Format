"""
Validation and normalization utilities for merged Excel data.

This module is pure: it operates only on the provided DataFrame and
does not perform any file I/O.
"""

from __future__ import annotations
import re
from decimal import Decimal, ROUND_HALF_UP
import random
from typing import Dict, List, Tuple, Iterable
import pandas as pd # type: ignore
try:
    import usaddress
except Exception:  # pragma: no cover - optional dependency
    usaddress = None

# ---------------------------------------------------------------------------
# Cleaning helpers
# ---------------------------------------------------------------------------

NULL_LIKE_STRINGS = {
    "nan",
    "na",
    "n/a",
    "null",
    "none",
    "nil",
    "",
    " ",
    "0",
    "0.0",
    "00:00:00",
    "--",
    "-",
    "###",
}

COUNTRY_ALIASES = {
    "usa": "United States",
    "united states": "United States",
    "us": "United States",
    "aus": "Australia",
    "australia": "Australia",
    "nz": "New Zealand",
    "new zealand": "New Zealand",
    "ca": "Canada",
    "can": "Canada",
    "canada": "Canada",
}

AUS_STATE_ABBREVS = {"ACT", "NSW", "NT", "QLD", "SA", "TAS", "VIC", "WA"}
NZ_STATE_ABBREVS = {
    "NL", "AKL", "WKO", "BOP", "GIS", "HKB", "TKI", "MWT", "WGN", "TAS",
    "NSN", "MBH", "WTC", "CAN", "OTA", "STL", "CIT", "COK", "NIU",
}
CA_STATE_ABBREVS = {"AB", "BC", "MB", "NB", "NL", "NS", "ON", "PE", "QC", "SK", "NT", "NU", "YT"}

NULL_COLUMNS = {
    "Prepaid Additional Rent/Premium",
    "Prepaid Tax",
    "Protection/Insurance Provider",
    "Protection/Insurance Coverage",
    "Additional Rent/Premium",
    "Delinquency Status",
    "Lien Status",
    "Lien Posted Date",
    "Promotion",
    "Promotion Type",
    "Promotion Value",
    "Promotion Start",
    "Promotion Length",
    "Discount",
    "Discount Type",
    "Discount Value",
    "Commanding Officer First Name",
    "Commanding Officer Last Name",
    "Commanding Officer Phone",
    "Commanding Officer Email",
    "Rank",
    "Military Serial Number",
    "Military Email",
    "Service Member DOB",
    "Expiration Term of Service",
    "Military Branch",
    "Military Unit Name",
    "Military Unit Phone",
    "Military Unit Address 1",
    "Military Unit Address 2",
    "Military City",
    "Military Unit State",
    "Military Unit Zipcode",
    "Lien Holder First Name",
    "Lien Holder Last Name",
    "Lien Holder Email",
    "Lien Holder Phone",
    "Lien Holder Address 1",
    "Lien Holder Address 2",
    "Lien Holder City",
    "Lien Holder State",
    "Lien Holder Zipcode",
    "Catch Flag",
    "Alarm Enabled",
    "24-Hour Access",
    "payment_cycle",
    "IsBusinessLease",
    "start_date",
    "pay_by_date",
    "end_date",
    "PaperlessBilling",
    "Offline",
    "OfflineReason",
    "smAgreeID",
    "smCustID",
    "smUnitID",
    "UnitStartDate",
    "WalkthoughOrder",
    "smUnitTypeID",
    "ConversionNote",
    "AutoPay",
    "AutoPayAmt",
}


def _normalize_null_like(value: object) -> object:
    if pd.isna(value):
        return None
    raw = str(value).strip()
    return None if raw.lower() in NULL_LIKE_STRINGS else value


def normalize_column_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.rename(columns=lambda x: str(x).strip(), inplace=True)
    return df


def map_phone_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    phone_mapping = {
        "Primary Phone": "Cell Phone",
        "Secondary Phone": "Home Phone",
        "Tertiary Phone": "Work Phone",
        "Space Size \n[width * length]": "Space Size",
    }
    df.rename(columns=phone_mapping, inplace=True)
    return df


def set_column_names_by_index(df: pd.DataFrame) -> pd.DataFrame:
    if not all(isinstance(c, int) for c in df.columns):
        return df
    if len(df.columns) < 82:
        return df
    df = df.copy()
    column_mapping = {
        0: "Owner",
        1: "Name",
        2: "Building",
        3: "Space",
        4: "Width",
        5: "Length",
        6: "Height",
        7: "Rate",
        8: "Web Rate",
        9: "Space Size",
        10: "Space Category",
        11: "Space Type",
        12: "Door Width",
        13: "Door Height",
        14: "Amenities",
        15: "Sq. Ft.",
        16: "Floor",
        17: "First Name",
        18: "Last Name",
        20: "Account Code",
        21: "Address",
        22: "City",
        23: "State",
        24: "ZIP",
        25: "Country",
        26: "Email",
        27: "Cell Phone",
        28: "Home Phone",
        29: "Work Phone",
        30: "Access Code",
        31: "DOB",
        32: "Gender",
        33: "Active Military",
        34: "DL Id",
        35: "DL State",
        36: "DL City",
        37: "DL Exp Date",
        38: "Rent",
        39: "Last Rent Change Date",
        40: "Move In Date",
        41: "Move Out Date",
        42: "Paid Date",
        43: "Bill Day",
        44: "Paid Through Date",
        45: "Alt First Name",
        46: "Alt Last Name",
        47: "Alt Middle Name",
        48: "Alt Address",
        49: "Alt City",
        50: "Alt State",
        51: "Alt ZIP",
        52: "Alt Email",
        53: "Alt Home Phone",
        54: "Alt Work Phone",
        55: "Alt Cell Phone",
        56: "Security Deposit",
        57: "Security Deposit Balance",
        58: "Rent Balance",
        59: "Fees Balance",
        60: "Protection/Insurance Balance",
        61: "Merchandise Balance",
        62: "Late Fees Balance",
        63: "Lien Fees Balance",
        64: "Tax Balance",
        65: "Prepaid Rent",
        66: "Prepaid Additional Rent/Premium",
        67: "Prepaid Tax",
        68: "Protection/Insurance Provider",
        69: "Protection/Insurance Coverage",
        70: "Additional Rent/Premium",
        71: "Delinquency Status",
        72: "Lien Status",
        73: "Lien Posted Date",
        74: "Promotion",
        75: "Promotion Type",
        76: "Promotion Value",
        77: "Promotion Start",
        78: "Promotion Length",
        79: "Discount",
        80: "Discount Type",
        81: "Discount Value",
    }
    for old_index, new_name in column_mapping.items():
        if old_index < len(df.columns):
            df.columns.values[old_index] = new_name
    if len(df.columns) > 19:
        if "Middle Name" in df.columns:
            df.columns.values[19] = "Middle Name"
        elif "Business" in df.columns:
            df.columns.values[19] = "Business"
        else:
            df.columns.values[19] = "Middle Name"
    return df


def add_missing_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    missing_columns = [
        "Commanding Officer First Name",
        "Commanding Officer Last Name",
        "Commanding Officer Phone",
        "Commanding Officer Email",
        "Rank",
        "Military Serial Number",
        "Military Email",
        "Service Member DOB",
        "Expiration Term of Service",
        "Military Branch",
        "Military Unit Name",
        "Military Unit Phone",
        "Military Unit Address 1",
        "Military Unit Address 2",
        "Military City",
        "Military Unit State",
        "Military Unit Zipcode",
        "Lien Holder First Name",
        "Lien Holder Last Name",
        "Lien Holder Email",
        "Lien Holder Phone",
        "Lien Holder Address 1",
        "Lien Holder Address 2",
        "Lien Holder City",
        "Lien Holder State",
        "Lien Holder Zipcode",
        "Catch Flag",
        "Alarm Enabled",
        "24-Hour Access",
        "payment_cycle",
        "IsBusinessLease",
        "start_date",
        "pay_by_date",
        "end_date",
        "PaperlessBilling",
        "Offline",
        "OfflineReason",
        "smAgreeID",
        "smCustID",
        "smUnitID",
        "UnitStartDate",
        "WalkthoughOrder",
        "smUnitTypeID",
        "ConversionNote",
        "AutoPay",
        "AutoPayAmt",
    ]
    for column_name in missing_columns:
        if column_name not in df.columns:
            df[column_name] = None
    return df


def replace_special_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    return df.replace(["#REF!", "#ERROR!"], "nan")


def replace_zero_dates(df: pd.DataFrame, date_cols: Iterable[str]) -> pd.DataFrame:
    df = df.copy()
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].replace(["0", 0, "00:00:00"], None)
    return df


def drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    mask = df.apply(lambda row: all(_is_missing(v) for v in row.values), axis=1)
    return df.loc[~mask].reset_index(drop=True)


def remove_numeric_suffix(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r"\.0$", "", regex=True)
    return df


def normalize_country(val: object) -> str | None:
    if pd.isna(val):
        return None
    raw = str(val).strip().lower()
    if not raw:
        return None
    return COUNTRY_ALIASES.get(raw, str(val).strip())

def clean_currency(val):
    """
    Normalize currency:
    - Removes $, commas, spaces, special chars
    - ONLY '-' indicates negative
    - Parentheses are ignored
    - Returns int when whole dollars, otherwise float with 2 decimals
    - Returns None for garbage input like '---', '$$$', '()'
    """
    if pd.isna(val):
        return None

    raw = str(val).strip()

    s = re.sub(r"[^0-9.]", "", raw)

    if not s:
        return 0

    negative = "-" in raw

    num = Decimal(s)
    if negative:
        num = -num

    quantized = num.quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)

    if quantized == quantized.to_integral_value():
        return int(quantized)

    return float(quantized)


def clean_number(val):
    """Standardize numeric input to int."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    s = re.sub(r"[^0-9.]", "", s)
    try:
        return int(s) if s else None
    except ValueError:
        return None


def clean_phoneNumber(value):
    """
    Validate US phone numbers.
    Accepts: XXXXXXXXXX, 1XXXXXXXXXX, +1XXXXXXXXXX
    Returns phone number as int (preserving country code 1 if present) if valid, else None.
    """
    if pd.isna(value):
        return None

    digits = re.sub(r"\D", "", str(value))

    if len(digits) == 11 and digits.startswith("1"):
        core_digits = digits[1:]
    elif len(digits) == 10:
        core_digits = digits
    else:
        return None

    if core_digits == "0000000000":
        return None

    area_code = core_digits[:3]
    exchange_code = core_digits[3:6]

    if area_code[0] in {"0", "1"}:
        return None

    if exchange_code[0] in {"0", "1"}:
        return None

    return int(digits)


def clean_date(val):
    """Safely parse dates to MM/DD/YY."""
    if pd.isna(val):
        return None
    dt = pd.to_datetime(val, errors="coerce")
    return dt.strftime("%m/%d/%y") if pd.notna(dt) else None


def clean_boolean(val):
    """Normalize boolean-like fields; leave blank when not explicitly truthy/falsy."""
    if pd.isna(val):
        return None
    normalized = str(val).strip().lower()
    if normalized in {"y", "yes", "true", "1"}:
        return True
    if normalized in {"n", "no", "false", "0"}:
        return False
    return None


EMAIL_PATTERN = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")


def is_valid_email(val: str) -> bool:
    """Return True if val looks like a basic email address."""
    return bool(EMAIL_PATTERN.fullmatch(val))


def clean_email(val):
    """
    Basic email validator:
    - Strips whitespace
    - Lowercases
    - Returns None if format is invalid
    """
    if pd.isna(val):
        return None
    s = str(val).strip().lower()
    if not s:
        return None
    return s if is_valid_email(s) else None


def clean_zip(val):
    if pd.isna(val):
        return val
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if re.fullmatch(r"\d{5}", s):
        return int(s)
    return val


def normalize_zip_from_usaddress(val: str) -> str | None:
    if not val:
        return None
    match = re.search(r"\d{5}", val)
    return match.group(0) if match else None


def parse_full_us_address(raw: str) -> Dict[str, str] | None:
    if not raw or usaddress is None:
        return None
    try:
        tagged, _ = usaddress.tag(raw)
    except Exception:
        return None
    city = tagged.get("PlaceName")
    state = tagged.get("StateName")
    zip_code = tagged.get("ZipCode")
    if not (city and state and zip_code):
        return None
    try:
        parts = usaddress.parse(raw)
    except Exception:
        return None
    street_tokens = [token for token, label in parts if label not in {"PlaceName", "StateName", "ZipCode"}]
    street = " ".join(street_tokens).strip()
    street = re.sub(r"\s+,", ",", street)
    street = re.sub(r"\s{2,}", " ", street).strip().strip(",")
    if not street:
        return None
    return {
        "street": street,
        "city": city.strip(),
        "state": state.strip(),
        "zip": zip_code.strip(),
    }


US_STATES_MAP = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
    "american samoa": "AS",
    "guam": "GU",
    "northern mariana islands": "MP",
    "puerto rico": "PR",
    "united states minor outlying islands": "UM",
    "virgin islands": "VI",
}
VALID_STATE_ABBREVS = set(US_STATES_MAP.values())


def normalize_state(val):
    """Normalize state name or abbreviation to 2-letter code."""
    if pd.isna(val):
        return None
    s = str(val).strip().lower()
    if not s:
        return None
    if s.upper() in VALID_STATE_ABBREVS:
        return s.upper()
    return US_STATES_MAP.get(s)


def is_valid_state_abbrev(val):
    return val in VALID_STATE_ABBREVS


def normalize_state_by_country(state_val: object, country_val: object) -> Tuple[object | None, bool]:
    """
    Normalize state based on country; returns (normalized_value, is_valid).
    For US: use normalize_state. For AU/NZ/CA: validate against known abbreviations.
    Otherwise: return original value without validation.
    """
    if _is_missing(state_val):
        return None, True
    country = normalize_country(country_val)
    raw_state = str(state_val).strip()
    if not country:
        norm = normalize_state(state_val)
        return (norm or state_val), bool(norm)
    country_lower = country.lower()
    if country_lower in {"united states", "usa"}:
        norm = normalize_state(state_val)
        return (norm or state_val), bool(norm)
    if country_lower == "australia":
        return raw_state.upper(), raw_state.upper() in AUS_STATE_ABBREVS
    if country_lower == "new zealand":
        return raw_state.upper(), raw_state.upper() in NZ_STATE_ABBREVS
    if country_lower == "canada":
        return raw_state.upper(), raw_state.upper() in CA_STATE_ABBREVS
    return raw_state, True


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------

_NAME_ALLOWED_PATTERN = re.compile(r"^[A-Za-z\s,'-]+$")
_NAME_TOKEN_PATTERN = re.compile(r"^[A-Za-z]+(?:[\'-][A-Za-z]+)*$")
_NAME_COMPANY_PATTERN = re.compile(
    r"\b(c\/o|care of|company|co|inc|inc\.|llc|l\.l\.c\.|corp|corp\.|ltd|ltd\.)\b",
    re.IGNORECASE,
)
_NAME_FIELD_ALLOWED = re.compile(r"^[A-Za-z\s&,'-]*$")

NAME_COLS = {
    "First Name",
    "Middle Name",
    "Last Name",
    "Alt First Name",
    "Alt Middle Name",
    "Alt Last Name",
    "Commanding Officer First Name",
    "Commanding Officer Last Name",
    "Lien Holder First Name",
    "Lien Holder Last Name",
}

SPACE_PATTERN = re.compile(r"^[A-Za-z0-9-]*$")

SPACE_SIZE_MATCH_PATTERN = re.compile(
    r"^\s*\[?\s*(\d+(?:\.\d+)?)\s*[xX]\s*(\d+(?:\.\d+)?)\s*\]?\s*$"
)


def normalize_name_fields(value: object) -> Dict[str, object]:
    """
    Normalize combined name strings into first/middle/last.
    Returns: {"first_name": str|None, "middle_name": str|None, "last_name": str|None, "is_valid": bool}
    """
    if pd.isna(value):
        return {"first_name": None, "middle_name": None, "last_name": None, "is_valid": True}

    raw = str(value).strip()
    if raw == "":
        return {"first_name": None, "middle_name": None, "last_name": None, "is_valid": True}

    if not _NAME_ALLOWED_PATTERN.fullmatch(raw):
        return {"first_name": None, "middle_name": None, "last_name": None, "is_valid": False}

    if _NAME_COMPANY_PATTERN.search(raw):
        return {"first_name": None, "middle_name": None, "last_name": None, "is_valid": False}

    if "," in raw:
        parts = [p.strip() for p in raw.split(",", 1)]
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return {"first_name": None, "middle_name": None, "last_name": None, "is_valid": False}
        last = parts[0]
        first_and_middle = parts[1].split()
        if not first_and_middle:
            return {"first_name": None, "middle_name": None, "last_name": None, "is_valid": False}
        if not _NAME_TOKEN_PATTERN.fullmatch(last):
            return {"first_name": None, "middle_name": None, "last_name": None, "is_valid": False}
        for token in first_and_middle:
            if not _NAME_TOKEN_PATTERN.fullmatch(token):
                return {"first_name": None, "middle_name": None, "last_name": None, "is_valid": False}
        first = first_and_middle[0]
        middle = " ".join(first_and_middle[1:]) if len(first_and_middle) > 1 else None
        return {"first_name": first, "middle_name": middle, "last_name": last, "is_valid": True}

    tokens = raw.split()
    if len(tokens) == 1:
        if not _NAME_TOKEN_PATTERN.fullmatch(tokens[0]):
            return {"first_name": None, "middle_name": None, "last_name": None, "is_valid": False}
        return {"first_name": tokens[0], "middle_name": None, "last_name": None, "is_valid": True}

    for token in tokens:
        if not _NAME_TOKEN_PATTERN.fullmatch(token):
            return {"first_name": None, "middle_name": None, "last_name": None, "is_valid": False}

    first = tokens[0]
    last = tokens[-1]
    middle = " ".join(tokens[1:-1]) if len(tokens) > 2 else None
    return {"first_name": first, "middle_name": middle, "last_name": last, "is_valid": True}


# ---------------------------------------------------------------------------
# Mapping-driven defaults
# ---------------------------------------------------------------------------

def _is_missing(val) -> bool:
    """Treat NaN/None/empty/whitespace as missing."""
    if pd.isna(val):
        return True
    try:
        return str(val).strip().lower() in NULL_LIKE_STRINGS
    except Exception:
        return False


def apply_default_values_from_mapping(df: pd.DataFrame, mapping_path: str) -> pd.DataFrame:
    """
    Apply default_value rows from mapping file to the DataFrame without overwriting non-null values.
    Business rule: if default_value is present AND (report_name is empty OR column_name is empty),
    treat row as static default column.
    """
    try:
        mapping_df = pd.read_excel(mapping_path, engine="openpyxl")
    except Exception:
        # If mapping cannot be read, return original df unchanged.
        return df

    required_cols = {"output_col", "report_name", "column_name", "default_value"}
    if not required_cols.issubset(set(mapping_df.columns)):
        return df

    df = df.copy()

    for _, row in mapping_df.iterrows():
        output_col = row.get("output_col")
        report_name = row.get("report_name")
        column_name = row.get("column_name")
        default_value = row.get("default_value")

        if _is_missing(output_col):
            continue

        has_default = not _is_missing(default_value)
        has_report = not _is_missing(report_name)
        has_column = not _is_missing(column_name)

        if not has_default:
            continue

        # Apply only when report or column is missing (static default column).
        if has_report and has_column:
            continue

        col_name = output_col
        if col_name in df.columns:
            mask = df[col_name].apply(_is_missing)
            if mask.any():
                df.loc[mask, col_name] = default_value
        else:
            df[col_name] = default_value

    return df


# ---------------------------------------------------------------------------
# Space Category parsing
# ---------------------------------------------------------------------------

SPACE_CATEGORY_PATTERN = re.compile(
    r"^\s*(\d+(?:\.\d+)?)\s*[xX]\s*(\d+(?:\.\d+)?)\s*[-]\s*(.+)$"
)


def parse_space_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract Width/Length/Space Type from 'Space Category' formatted as '5X5-SelfStorage'.
    Populates missing Width/Length/Space Type, clears the original 'Space Category' entry.
    """
    if "Space Category" not in df.columns:
        return df

    df = df.copy()
    if "Width" not in df.columns:
        df["Width"] = None
    if "Length" not in df.columns:
        df["Length"] = None
    if "Space Type" not in df.columns:
        df["Space Type"] = None

    for idx, raw in df["Space Category"].items():
        if _is_missing(raw):
            continue
        match = SPACE_CATEGORY_PATTERN.match(str(raw))
        if not match:
            continue
        width_val, length_val, storage_type = match.groups()
        if _is_missing(df.at[idx, "Width"]):
            try:
                df.at[idx, "Width"] = float(width_val)
            except Exception:
                df.at[idx, "Width"] = width_val
        if _is_missing(df.at[idx, "Length"]):
            try:
                df.at[idx, "Length"] = float(length_val)
            except Exception:
                df.at[idx, "Length"] = length_val
        if _is_missing(df.at[idx, "Space Type"]):
            df.at[idx, "Space Type"] = storage_type.strip()
        # Clear the parsed source to avoid duplicative data.
        df.at[idx, "Space Category"] = None
    return df


# ---------------------------------------------------------------------------
# Space Size parsing (from user-entered strings)
# ---------------------------------------------------------------------------

SPACE_SIZE_PATTERN = re.compile(
    r"^\s*(\d+(?:\.\d+)?)\s*[xX]\s*(\d+(?:\.\d+)?)(?:\s*[xX]\s*(\d+(?:\.\d+)?))?(?:\s*[^0-9].*)?$"
)


def parse_space_size(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse 'Space Size' values like '3 X 4 H&C', '8 X 7 X 6', or '10X30DU' and normalize:
    - Extract Width/Length (and Height when provided) if missing
    - Rewrite Space Size as '[Width x Length]'
    """
    if "Space Size" not in df.columns:
        return df

    df = df.copy()
    if "Width" not in df.columns:
        df["Width"] = None
    if "Length" not in df.columns:
        df["Length"] = None

    if "_space_size_parsed" not in df.columns:
        df["_space_size_parsed"] = False

    for idx, raw in df["Space Size"].items():
        if _is_missing(raw):
            continue
        if not (_is_missing(df.at[idx, "Width"]) and _is_missing(df.at[idx, "Length"])):
            continue
        match = SPACE_SIZE_PATTERN.match(str(raw))
        if not match:
            continue
        width_val, length_val, height_val = match.groups()
        if _is_missing(df.at[idx, "Width"]):
            try:
                df.at[idx, "Width"] = float(width_val)
            except Exception:
                df.at[idx, "Width"] = width_val
        if _is_missing(df.at[idx, "Length"]):
            try:
                df.at[idx, "Length"] = float(length_val)
            except Exception:
                df.at[idx, "Length"] = length_val
        if height_val is not None:
            if "Height" not in df.columns:
                df["Height"] = None
            if _is_missing(df.at[idx, "Height"]):
                try:
                    df.at[idx, "Height"] = float(height_val)
                except Exception:
                    df.at[idx, "Height"] = height_val

        # Normalize Space Size format based on parsed values.
        def _fmt(val):
            try:
                num = float(val)
                if num.is_integer():
                    return str(int(num))
                return str(num).rstrip("0").rstrip(".")
            except Exception:
                return str(val)

        df.at[idx, "Space Size"] = f"[{_fmt(df.at[idx, 'Width'])} x {_fmt(df.at[idx, 'Length'])}]"
        df.at[idx, "_space_size_parsed"] = True

    return df


# ---------------------------------------------------------------------------
# Space Type parsing (dimensions embedded in type strings)
# ---------------------------------------------------------------------------

SPACE_TYPE_DIMENSION_PATTERN = re.compile(
    r"^\s*(\d+(?:\.\d+)?)\s*[xX]\s*(\d+(?:\.\d+)?)(?:\s*[A-Za-z].*)?$"
)


def parse_space_type_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse 'Space Type' values like '10X30DU' to fill Width/Length if missing.
    """
    if "Space Type" not in df.columns:
        return df

    df = df.copy()
    if "Width" not in df.columns:
        df["Width"] = None
    if "Length" not in df.columns:
        df["Length"] = None

    for idx, raw in df["Space Type"].items():
        if _is_missing(raw):
            continue
        if not (_is_missing(df.at[idx, "Width"]) and _is_missing(df.at[idx, "Length"])):
            continue
        match = SPACE_TYPE_DIMENSION_PATTERN.match(str(raw))
        if not match:
            continue
        width_val, length_val = match.groups()
        if _is_missing(df.at[idx, "Width"]):
            try:
                df.at[idx, "Width"] = float(width_val)
            except Exception:
                df.at[idx, "Width"] = width_val
        if _is_missing(df.at[idx, "Length"]):
            try:
                df.at[idx, "Length"] = float(length_val)
            except Exception:
                df.at[idx, "Length"] = length_val

    return df


# ---------------------------------------------------------------------------
# Column classification
# ---------------------------------------------------------------------------

PHONE_COLS = {
    "Cell Phone",
    "Home Phone",
    "Work Phone",
    "Alt Home Phone",
    "Alt Work Phone",
    "Alt Cell Phone",
    "Lien Holder Phone",
    "Commanding Officer Phone",
    "Military Unit Phone",
}

CURRENCY_COLS = {
    "Rate",
    "Web Rate",
    "Rent",
    "Security Deposit",
    "Security Deposit Balance",
    "Rent Balance",
    "Fees Balance",
    "Protection/Insurance Balance",
    "Merchandise Balance",
    "Late Fees Balance",
    "Lien Fees Balance",
    "Tax Balance",
    "Prepaid Rent",
    "Prepaid Amount",
    "Prepaid Additional Rent/Premium",
    "Prepaid Tax",
    "Additional Rent/Premium",
    "Discount Value",
    "Promotion Value",
    "AutoPayAmt",
    "Protection/Insurance Coverage",
}

NUMBER_COLS = {
    "Width",
    "Length",
    "Height",
    "Door Width",
    "Door Height",
    "Promotion Length",
    "Account Code",
    "Access Code",
    "Sq. Ft.",
}

DATE_COLS = {
    "DOB",
    "DL Exp Date",
    "Last Rent Change Date",
    "Move In Date",
    "Move Out Date",
    "Paid Date",
    "Paid Through Date",
    "Lien Posted Date",
    "Promotion Start",
    "start_date",
    "pay_by_date",
    "end_date",
    "UnitStartDate",
}

BOOLEAN_COLS = {
    "Active Military",
    "PaperlessBilling",
    "Offline",
    "Alarm Enabled",
    "24-hour access",
    "24-Hour Access",
    "IsBusinessLease",
    "Catch Flag",
    "AutoPay",
}

EMAIL_COLS = {
    "Email",
    "Alt Email",
    "Lien Holder Email",
    "Commanding Officer Email",
    "Military Email",
}

STATE_COLS = {
    "State",
    "DL State",
    "Alt State",
    "Lien Holder State",
    "Military Unit State",
}

ZIP_COLS = {"ZIP", "Alt ZIP", "Lien Holder Zipcode", "Military Unit Zipcode"}
ADDRESS_COLS = {
    "Address",
    "Street Address",
    "Street",
    "Mailing Address",
    "Address Line 1",
    "Address1",
    "Alt Address",
}
# ---------------------------------------------------------------------------
# Derived column logic
# ---------------------------------------------------------------------------

def compute_space_size(row):
    """Build a size like [Width x Length] (falls back to Height if Length is missing)."""
    w = row.get("Width")
    l = row.get("Length")

    def fmt(val):
        try:
            num = float(val)
            if num.is_integer():
                return str(int(num))
            return str(num).rstrip("0").rstrip(".")
        except Exception:
            return str(val)

    if pd.notna(w) and pd.notna(l):
        return f"[{fmt(w)} x {fmt(l)}]"
    return None

def compute_bill_day(val):
    """Return next day's day-of-month (MM/DD based paid-through date), formatted as two digits."""
    if pd.isna(val) or str(val).strip() == "":
        return None
    dt = pd.to_datetime(val, errors="coerce")
    if pd.isna(dt):
        return None
    next_day = dt + pd.Timedelta(days=1)
    return int(f"{next_day.day:02d}")

# ---------------------------------------------------------------------------
# Main normalization pipeline
# ---------------------------------------------------------------------------

def normalize_dataframe(
    df: pd.DataFrame, mapping_path: str, mig_date: str | None = None
) -> Tuple[pd.DataFrame, Dict[str, List[int]], Dict[str, Dict[str, List[int]]], List[Dict[str, object]]]:
    """
    Accepts a merged DataFrame
    Applies mapping-driven defaults, validation, normalization, and derived column logic
    Returns:
        - cleaned DataFrame
        - invalid_cells: dict[column_name] -> list[row_index]
        - highlight_cells: dict[color] -> dict[column_name] -> list[row_index] (for informational highlighting)
        - mig_date: optional migration date for paid-through validations (YYYY-MM-DD)
    """
    df = normalize_column_headers(df)
    df = map_phone_columns(df)
    df = set_column_names_by_index(df)
    df = add_missing_columns(df)
    df = replace_special_values(df)
    df = replace_zero_dates(df, DATE_COLS)
    for col in NULL_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: None if _is_missing(v) else v)
    df = drop_empty_rows(df)
    df = remove_numeric_suffix(df, NUMBER_COLS.union(ZIP_COLS))
    df = apply_default_values_from_mapping(df, mapping_path)
    df = parse_space_category(df)
    df = parse_space_size(df)
    df = parse_space_type_dimensions(df)
    df = df.copy()
    invalid_cells: Dict[str, List[int]] = {}
    highlight_cells: Dict[str, Dict[str, List[int]]] = {"red": {}, "blue": {}, "dark_red": {}, "yellow": {}}
    invalid_reasons: List[Dict[str, object]] = []
    address_cols = [col for col in df.columns if col in ADDRESS_COLS]
    if address_cols:
        if "City" not in df.columns:
            df["City"] = None
        if "State" not in df.columns:
            df["State"] = None
        if "ZIP" not in df.columns:
            df["ZIP"] = None
        for col in address_cols:
            for idx, v in df[col].items():
                if _is_missing(v):
                    continue
                parsed = parse_full_us_address(str(v).strip())
                if not parsed:
                    continue
                if _is_missing(df.at[idx, "City"]):
                    df.at[idx, "City"] = parsed["city"]
                if _is_missing(df.at[idx, "State"]):
                    df.at[idx, "State"] = parsed["state"]
                if _is_missing(df.at[idx, "ZIP"]):
                    zip_clean = normalize_zip_from_usaddress(parsed["zip"])
                    df.at[idx, "ZIP"] = zip_clean if zip_clean else parsed["zip"]
                df.at[idx, col] = parsed["street"]

    def _get_space_value(idx: int) -> object:
        if "Space" in df.columns:
            return df.at[idx, "Space"]
        return None

    def add_invalid_reason(idx: int, col: str, value: object, reason: str) -> None:
        invalid_reasons.append(
            {
                "row_index": idx,
                "space": _get_space_value(idx),
                "column": col,
                "value": value,
                "reason": reason,
            }
        )

    for col in df.columns:
        if col == "First Name":
            # Split combined name formats; flag invalid name strings.
            col_values = []
            invalid_idx = []
            if "Middle Name" not in df.columns:
                df["Middle Name"] = None
            if "Last Name" not in df.columns:
                df["Last Name"] = None
            for idx, v in df[col].items():
                if pd.isna(v) or str(v).strip() == "":
                    col_values.append(None)
                    continue
                normalized = normalize_name_fields(v)
                if normalized["is_valid"]:
                    col_values.append(normalized["first_name"])
                    if normalized["middle_name"] and _is_missing(df.at[idx, "Middle Name"]):
                        df.at[idx, "Middle Name"] = normalized["middle_name"]
                    if normalized["last_name"] and _is_missing(df.at[idx, "Last Name"]):
                        df.at[idx, "Last Name"] = normalized["last_name"]
                else:
                    col_values.append(v)
                    invalid_idx.append(idx)
                    add_invalid_reason(idx, col, v, "Invalid name format")
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

        elif col in NAME_COLS:
            # Validate name fields for allowed characters.
            col_values = []
            invalid_idx = []
            for idx, v in df[col].items():
                if _is_missing(v):
                    col_values.append(None)
                    continue
                s = str(v).strip()
                if _NAME_FIELD_ALLOWED.fullmatch(s):
                    col_values.append(s)
                else:
                    col_values.append(v)
                    invalid_idx.append(idx)
                    add_invalid_reason(idx, col, v, "Invalid name characters")
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

        elif col in PHONE_COLS:
            # Validate and normalize phone numbers; flag invalid formats.
            col_values = []
            invalid_idx = []
            for idx, v in df[col].items():
                if pd.isna(v) or str(v).strip() == "":
                    col_values.append(None)
                    continue
                cleaned = clean_phoneNumber(v)
                if cleaned is not None:
                    col_values.append(cleaned)
                else:
                    col_values.append(v)
                    invalid_idx.append(idx)
                    add_invalid_reason(idx, col, v, "Invalid phone format")
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values
        

        elif col in CURRENCY_COLS:
            # Normalize currency values; flag invalid currency formats.
            col_values = []
            invalid_idx = []
            for idx, v in df[col].items():
                if pd.isna(v) or str(v).strip() == "":
                    col_values.append(None)
                    continue
                cleaned = clean_currency(v)
                if cleaned is not None:
                    col_values.append(cleaned)
                else:
                    col_values.append(v)
                    invalid_idx.append(idx)
                    add_invalid_reason(idx, col, v, "Invalid currency format")
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

        elif col == "Country":
            col_values = []
            for _, v in df[col].items():
                col_values.append(normalize_country(v))
            df[col] = col_values

        elif col in NUMBER_COLS:
            # Normalize numeric fields; flag non-numeric inputs.
            col_values = []
            invalid_idx = []
            for idx, v in df[col].items():
                if pd.isna(v) or str(v).strip() == "":
                    col_values.append(None)
                    continue
                if col in {"Width", "Length"} and "_space_size_parsed" in df.columns:
                    if bool(df.at[idx, "_space_size_parsed"]):
                        if isinstance(v, (int, float)):
                            col_values.append(v)
                            continue
                cleaned = clean_number(v)
                if cleaned is not None:
                    col_values.append(cleaned)
                else:
                    col_values.append(v)
                    invalid_idx.append(idx)
                    add_invalid_reason(idx, col, v, "Invalid number")
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

        elif col in DATE_COLS:
            # Normalize dates to MM/DD/YY; flag invalid date values.
            col_values = []
            invalid_idx = []
            for idx, v in df[col].items():
                if pd.isna(v) or str(v).strip() == "":
                    col_values.append(None)
                    continue
                cleaned = clean_date(v)
                if cleaned is not None:
                    col_values.append(cleaned)
                else:
                    col_values.append(v)
                    invalid_idx.append(idx)
                    add_invalid_reason(idx, col, v, "Invalid date")
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

        
        elif col in BOOLEAN_COLS:
            df[col] = df[col].apply(clean_boolean)

        elif col in EMAIL_COLS:
            # Lowercase and validate emails; flag invalid addresses.
            col_values = []
            invalid_idx = []
            for idx, v in df[col].items():
                if pd.isna(v) or str(v).strip() == "":
                    col_values.append(None)
                    continue
                lowered = str(v).strip().lower()
                if is_valid_email(lowered):
                    col_values.append(lowered)
                else:
                    col_values.append(v)
                    invalid_idx.append(idx)
                    add_invalid_reason(idx, col, v, "Invalid email format")
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

        elif col in STATE_COLS:
            # Normalize state names/abbreviations; flag invalid states.
            col_values = []
            invalid_idx = []
            for idx, v in df[col].items():
                if _is_missing(v):
                    col_values.append(None)
                    continue
                country_val = df.at[idx, "Country"] if "Country" in df.columns else None
                norm, is_valid = normalize_state_by_country(v, country_val)
                col_values.append(norm)
                if not is_valid:
                    invalid_idx.append(idx)
                    add_invalid_reason(idx, col, v, "Invalid state for country")
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

        elif col == "Space Type":
            # Normalize/overwrite: classify parking vs storage when present, else leave None.
            df[col] = df[col].apply(
                lambda x: "Parking"
                if pd.notna(x) and "parking" in str(x).lower()
                else ("Storage" if pd.notna(x) and str(x).strip() != "" else None)
            )

        elif col in ZIP_COLS:
            # Normalize ZIP to 5 digits; flag non-5-digit values.
            col_values = []
            invalid_idx = []
            for idx, v in df[col].items():
                if _is_missing(v):
                    col_values.append(None)
                    continue
                country_val = df.at[idx, "Country"] if "Country" in df.columns else None
                country = normalize_country(country_val) or ""
                s = str(v).strip().replace(" ", "")
                if s.endswith(".0"):
                    s = s[:-2]
                if country.lower() in {"australia", "new zealand"}:
                    if re.fullmatch(r"\d{4}", s):
                        col_values.append(s)
                    else:
                        col_values.append(v)
                        invalid_idx.append(idx)
                        add_invalid_reason(idx, col, v, "Invalid ZIP (expected 4 digits)")
                elif country.lower() == "canada":
                    if len(s) == 6:
                        col_values.append(s)
                    else:
                        col_values.append(v)
                        invalid_idx.append(idx)
                        add_invalid_reason(idx, col, v, "Invalid ZIP (expected 6 characters)")
                else:
                    if re.fullmatch(r"\d{5}", s):
                        col_values.append(clean_zip(s))
                    else:
                        col_values.append(v)
                        invalid_idx.append(idx)
                        add_invalid_reason(idx, col, v, "Invalid ZIP (expected 5 digits)")
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

    if "Space" in df.columns:
        space_invalid = []
        for idx, v in df["Space"].items():
            if _is_missing(v):
                width_val = df.at[idx, "Width"] if "Width" in df.columns else None
                length_val = df.at[idx, "Length"] if "Length" in df.columns else None
                height_val = df.at[idx, "Height"] if "Height" in df.columns else None
                if not (_is_missing(width_val) and _is_missing(length_val) and _is_missing(height_val)):
                    space_invalid.append(idx)
                    add_invalid_reason(idx, "Space", v, "Space missing but dimensions provided")
            else:
                s = str(v).strip()
                if not SPACE_PATTERN.fullmatch(s):
                    space_invalid.append(idx)
                    add_invalid_reason(idx, "Space", v, "Invalid space format")
        if space_invalid:
            invalid_cells["Space"] = sorted(set(invalid_cells.get("Space", []) + space_invalid))

        if df["Space"].duplicated(keep=False).any():
            dup_idx = [idx for idx, dup in df["Space"].duplicated(keep=False).items() if dup]
            if dup_idx:
                invalid_cells["Space"] = sorted(set(invalid_cells.get("Space", []) + dup_idx))
                for idx in dup_idx:
                    add_invalid_reason(idx, "Space", df.at[idx, "Space"], "Duplicate space")

    if "Width" in df.columns:
        width_vals = pd.to_numeric(df["Width"], errors="coerce")
        width_invalid = []
        for idx, val in width_vals.items():
            if pd.isna(val):
                continue
            if not (0.0 < float(val) < 100.0):
                width_invalid.append(idx)
                add_invalid_reason(idx, "Width", df.at[idx, "Width"], "Width out of range")
        if width_invalid:
            invalid_cells["Width"] = sorted(set(invalid_cells.get("Width", []) + width_invalid))

    if "Length" in df.columns:
        length_vals = pd.to_numeric(df["Length"], errors="coerce")
        length_invalid = []
        for idx, val in length_vals.items():
            if pd.isna(val):
                continue
            if not (0.0 < float(val) < 101.0):
                length_invalid.append(idx)
                add_invalid_reason(idx, "Length", df.at[idx, "Length"], "Length out of range")
        if length_invalid:
            invalid_cells["Length"] = sorted(set(invalid_cells.get("Length", []) + length_invalid))

    if "Space Size" in df.columns and "Width" in df.columns and "Length" in df.columns:
        size_invalid = []
        for idx, v in df["Space Size"].items():
            if _is_missing(v):
                continue
            match = SPACE_SIZE_MATCH_PATTERN.match(str(v))
            if not match:
                size_invalid.append(idx)
                add_invalid_reason(idx, "Space Size", v, "Invalid space size format")
                continue
            width_val, length_val = match.groups()
            expected_w = pd.to_numeric(df.at[idx, "Width"], errors="coerce")
            expected_l = pd.to_numeric(df.at[idx, "Length"], errors="coerce")
            if pd.notna(expected_w) and pd.notna(expected_l):
                try:
                    if float(width_val) != float(expected_w) or float(length_val) != float(expected_l):
                        size_invalid.append(idx)
                        add_invalid_reason(idx, "Space Size", v, "Space Size does not match Width/Length")
                except Exception:
                    continue
        if size_invalid:
            invalid_cells["Space Size"] = sorted(set(invalid_cells.get("Space Size", []) + size_invalid))

    if "DL Id" in df.columns:
        dl_invalid = []
        for idx, v in df["DL Id"].items():
            if _is_missing(v):
                continue
            if not re.fullmatch(r"[A-Za-z0-9]+", str(v).strip()):
                dl_invalid.append(idx)
                add_invalid_reason(idx, "DL Id", v, "Invalid DL Id format")
        if dl_invalid:
            invalid_cells["DL Id"] = sorted(set(invalid_cells.get("DL Id", []) + dl_invalid))

    if all(col in df.columns for col in ("DL State", "DL City", "DL Exp Date", "DL Id")):
        missing_dl_id = []
        for idx in df.index:
            if _is_missing(df.at[idx, "DL Id"]):
                if any(
                    not _is_missing(df.at[idx, col])
                    for col in ("DL State", "DL City", "DL Exp Date")
                ):
                    missing_dl_id.append(idx)
                    add_invalid_reason(idx, "DL Id", df.at[idx, "DL Id"], "DL Id missing but other DL fields exist")
        if missing_dl_id:
            invalid_cells["DL Id"] = sorted(set(invalid_cells.get("DL Id", []) + missing_dl_id))

    if "Promotion" in df.columns:
        required_fields = ["Promotion Type", "Promotion Value", "Promotion Start", "Promotion Length"]
        promo_invalid: Dict[str, List[int]] = {}
        for idx, v in df["Promotion"].items():
            has_promo = not _is_missing(v)
            for field in required_fields:
                if field not in df.columns:
                    continue
                if has_promo and _is_missing(df.at[idx, field]):
                    promo_invalid.setdefault(field, []).append(idx)
                    add_invalid_reason(idx, field, df.at[idx, field], "Required for promotion")
                if not has_promo and not _is_missing(df.at[idx, field]):
                    promo_invalid.setdefault(field, []).append(idx)
                    add_invalid_reason(idx, field, df.at[idx, field], "Promotion fields present without promotion")
        for field, rows in promo_invalid.items():
            invalid_cells[field] = sorted(set(invalid_cells.get(field, []) + rows))

    if "Discount" in df.columns:
        discount_invalid: Dict[str, List[int]] = {}
        number_pattern = re.compile(r"^[-+]?\d*(?:\.\d+)?$")
        for idx, v in df["Discount"].items():
            has_discount = not _is_missing(v)
            discount_type = df.at[idx, "Discount Type"] if "Discount Type" in df.columns else None
            discount_value = df.at[idx, "Discount Value"] if "Discount Value" in df.columns else None
            if has_discount:
                if _is_missing(discount_type) or str(discount_type).strip().lower() not in {"fixed", "$", "%"}:
                    discount_invalid.setdefault("Discount Type", []).append(idx)
                    add_invalid_reason(idx, "Discount Type", discount_type, "Invalid discount type")
                if _is_missing(discount_value) or not number_pattern.fullmatch(str(discount_value).strip()):
                    discount_invalid.setdefault("Discount Value", []).append(idx)
                    add_invalid_reason(idx, "Discount Value", discount_value, "Invalid discount value")
            else:
                if not _is_missing(discount_type) or not _is_missing(discount_value):
                    discount_invalid.setdefault("Discount Type", []).append(idx)
                    discount_invalid.setdefault("Discount Value", []).append(idx)
                    add_invalid_reason(idx, "Discount", v, "Discount fields present without discount")
        for field, rows in discount_invalid.items():
            invalid_cells[field] = sorted(set(invalid_cells.get(field, []) + rows))

    if "Move In Date" in df.columns and "Paid Date" in df.columns:
        # Fill missing Move In Date with Paid Date when available.
        missing_move_in_mask = df["Move In Date"].apply(_is_missing)
        paid_present_mask = ~df["Paid Date"].apply(_is_missing)
        df.loc[missing_move_in_mask & paid_present_mask, "Move In Date"] = df.loc[
            missing_move_in_mask & paid_present_mask, "Paid Date"
        ]
        move_in_filled = [idx for idx, filled in (missing_move_in_mask & paid_present_mask).items() if filled]
        if move_in_filled:
            highlight_cells["yellow"]["Move In Date"] = move_in_filled

    if "First Name" in df.columns and "Last Name" in df.columns:
        # Derive Status from presence of occupant name.
        df["Status"] = df.apply(
            lambda row: "Occupied"
            if (pd.notna(row["First Name"]) and str(row["First Name"]).strip() != "")
            or (pd.notna(row["Last Name"]) and str(row["Last Name"]).strip() != "")
            else "Vacant",
            axis=1,
        )
        if "full_name" not in df.columns:
            df["full_name"] = (
                df["First Name"].fillna("").astype(str).str.strip()
                + " "
                + df["Last Name"].fillna("").astype(str).str.strip()
            ).str.strip().str.lower()

    if "Status" in df.columns:
        # Drop rows that are Vacant and missing Space.
        space_missing = (
            df["Space"].apply(_is_missing) if "Space" in df.columns else pd.Series(False, index=df.index)
        )
        first_missing = (
            df["First Name"].apply(_is_missing) if "First Name" in df.columns else pd.Series(False, index=df.index)
        )
        last_missing = (
            df["Last Name"].apply(_is_missing) if "Last Name" in df.columns else pd.Series(False, index=df.index)
        )
        drop_mask = df["Status"].eq("Vacant") & space_missing
        if drop_mask.any():
            keep_index = df.index[~drop_mask]
            index_map = {old_idx: new_idx for new_idx, old_idx in enumerate(keep_index)}
            df = df.loc[keep_index].reset_index(drop=True)

            # Remap validation indices to the new row positions.
            remapped_invalid = {}
            for col, idx_list in invalid_cells.items():
                new_idx = [index_map[i] for i in idx_list if i in index_map]
                if new_idx:
                    remapped_invalid[col] = new_idx
            invalid_cells = remapped_invalid

            remapped_highlight = {"red": {}, "blue": {}, "dark_red": {}, "yellow": {}}
            for color, col_map in highlight_cells.items():
                remapped_cols = {}
                for col, idx_list in col_map.items():
                    new_idx = [index_map[i] for i in idx_list if i in index_map]
                    if new_idx:
                        remapped_cols[col] = new_idx
                if remapped_cols:
                    remapped_highlight[color] = remapped_cols
            highlight_cells = remapped_highlight

            remapped_reasons = []
            for reason in invalid_reasons:
                old_idx = reason.get("row_index")
                if old_idx in index_map:
                    updated = dict(reason)
                    updated["row_index"] = index_map[old_idx]
                    remapped_reasons.append(updated)
            invalid_reasons = remapped_reasons

    if "full_name" in df.columns and "Address" in df.columns:
        full_dup = df["full_name"].duplicated(keep=False)
        addr_dup = df["Address"].duplicated(keep=False)
        dup_idx = [idx for idx in df.index if full_dup.get(idx, False) and addr_dup.get(idx, False)]
        if dup_idx:
            invalid_cells.setdefault("full_name", [])
            invalid_cells["full_name"] = sorted(set(invalid_cells["full_name"] + dup_idx))
            for idx in dup_idx:
                add_invalid_reason(idx, "full_name", df.at[idx, "full_name"], "Duplicate full name and address")

    
    if "Status" in df.columns:
        if "Paid Date" in df.columns:
            # Paid through date = paid date + 1 month - 1 day
            paid_through_filled: List[int] = []
            def _calc_paid_through(row):
                if row.get("Status") != "Occupied":
                    return row.get("Paid Through Date")
                if not _is_missing(row.get("Paid Through Date")):
                    return row.get("Paid Through Date")
                paid_date = pd.to_datetime(row.get("Paid Date"), errors="coerce")
                if pd.isna(paid_date):
                    return None
                paid_through = paid_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)
                paid_through_filled.append(row.name)
                return paid_through.strftime("%m/%d/%y")

            df["Paid Through Date"] = df.apply(_calc_paid_through, axis=1)
            if paid_through_filled:
                highlight_cells["yellow"]["Paid Through Date"] = paid_through_filled
            
    if "Status" in df.columns:
        if "Paid Date" in df.columns and "Paid Through Date" in df.columns:
            # Paid date = paid through date - 1 month + 1 day
            paid_date_filled: List[int] = []
            def _calc_paid_date(row):
                if row.get("Status") != "Occupied":
                    return row.get("Paid Date")
                if not _is_missing(row.get("Paid Date")):
                    return row.get("Paid Date")
                paid_through = pd.to_datetime(row.get("Paid Through Date"), errors="coerce")
                if pd.isna(paid_through):
                    return None
                paid_date = paid_through - pd.DateOffset(months=1) + pd.DateOffset(days=1)
                paid_date_filled.append(row.name)
                return paid_date.strftime("%m/%d/%y")

            df["Paid Date"] = df.apply(_calc_paid_date, axis=1)
            if paid_date_filled:
                highlight_cells["yellow"]["Paid Date"] = paid_date_filled

    if "Status" in df.columns:
        floor_col = next(
            (col for col in df.columns if str(col).strip().lower() == "floor"),
            None,
        )
        if floor_col:
            floor_idx = list(df.columns).index(floor_col)
            after_cols = [
                col
                for col in list(df.columns)[floor_idx + 1 :]
                if not str(col).startswith("_")
            ]
            if after_cols:
                vacant_missing_after = []
                for idx, row in df.iterrows():
                    status_value = str(row.get("Status") or "").strip().lower()
                    if status_value != "vacant":
                        continue
                    if all(_is_missing(row.get(col)) for col in after_cols):
                        vacant_missing_after.append(idx)
                if vacant_missing_after:
                    row_highlight = highlight_cells.setdefault("dark_red", {})
                    for col in df.columns:
                        row_highlight.setdefault(col, []).extend(vacant_missing_after)

    if "Status" in df.columns:
        # Highlight occupied rows missing required paid dates.
        occupied_mask = df["Status"].eq("Occupied")

        for paid_col in ("Paid Through Date", "Paid Date"):
            if paid_col in df.columns:
                missing_mask = df[paid_col].apply(_is_missing) & occupied_mask
                if missing_mask.any():
                    highlight_cells["red"][paid_col] = [
                        idx for idx, missing in missing_mask.items() if missing
                    ]
                    for idx, missing in missing_mask.items():
                        if missing:
                            add_invalid_reason(idx, paid_col, None, "Missing required value for occupied status")
    
    
        
    # -----------------------------------------------------------------------
    # Access Code resolution (derivation/generation + marking)
    # -----------------------------------------------------------------------
    def _extract_digits(val: object) -> str | None:
        if pd.isna(val):
            return None
        digits = re.sub(r"\D", "", str(val))
        return digits if digits else None

    def _last4(digits: str | None) -> str | None:
        if not digits or len(digits) < 4:
            return None
        return digits[-4:]

    def _generate_unique_access_code(existing: set[str]) -> str:
        for _ in range(10000):
            code = f"{random.randint(1000, 9999)}"
            if code in existing:
                continue
            existing.add(code)
            return code
        raise RuntimeError("Failed to generate a unique access code.")

    def _has_name(idx: int) -> bool:
        first = df.at[idx, "First Name"] if "First Name" in df.columns else None
        last = df.at[idx, "Last Name"] if "Last Name" in df.columns else None
        return not _is_missing(first) or not _is_missing(last)

    if "Access Code" not in df.columns:
        # Ensure Access Code column exists for derivation.
        df["Access Code"] = None
    if "Account Code" not in df.columns:
        df["Account Code"] = None

    used_access_codes: set[str] = set()
    if "Access Code" in df.columns:
        for val in df["Access Code"]:
            digits = _extract_digits(val)
            last4 = _last4(digits)
            if last4:
                used_access_codes.add(last4)

    if "Access Code" in df.columns:
        dummy_counter = 1
        # Populate missing access codes for occupied units (phone last4 or unique random).
        access_code_rows: List[int] = []
        for idx in df.index:
            # Only derive/populate for occupied units.
            status_val = df.at[idx, "Status"] if "Status" in df.columns else None
            if status_val != "Occupied":
                continue

            current_access = df.at[idx, "Access Code"]
            current_account = df.at[idx, "Account Code"]

            cell_digits = _extract_digits(df.at[idx, "Cell Phone"]) if "Cell Phone" in df.columns else None
            alt_digits = _extract_digits(df.at[idx, "Alt Cell Phone"]) if "Alt Cell Phone" in df.columns else None

            access_digits = _extract_digits(current_access)
            account_digits = _extract_digits(current_account)

            # If both missing, attempt phone-based fill.
            if _is_missing(current_access) and _is_missing(current_account):
                phone_digits = cell_digits or alt_digits
                if phone_digits:
                    df.at[idx, "Account Code"] = phone_digits
                    df.at[idx, "Access Code"] = _last4(phone_digits)
                elif _has_name(idx):
                    dummy_code = str(dummy_counter) * 4
                    dummy_counter += 1
                    df.at[idx, "Account Code"] = dummy_code
                    df.at[idx, "Access Code"] = dummy_code
                    access_code_rows.append(idx)

            # Account missing but Access present.
            if _is_missing(current_account) and not _is_missing(current_access):
                if cell_digits or alt_digits:
                    df.at[idx, "Account Code"] = (cell_digits or alt_digits)
                else:
                    df.at[idx, "Account Code"] = current_access
                access_code_rows.append(idx)

            # Access missing but Account present.
            if _is_missing(current_access) and not _is_missing(current_account):
                phone_last4 = _last4(cell_digits) or _last4(alt_digits)
                if phone_last4:
                    df.at[idx, "Access Code"] = phone_last4
                elif _has_name(idx):
                    access_code_val = _generate_unique_access_code(used_access_codes)
                    df.at[idx, "Access Code"] = access_code_val
                access_code_rows.append(idx)

            # Track used access codes.
            access_digits = _extract_digits(df.at[idx, "Access Code"])
            if access_digits:
                used_access_codes.add(_last4(access_digits) or access_digits)

        if access_code_rows:
            highlight_cells["yellow"]["Access Code"] = access_code_rows
            highlight_cells["yellow"]["Account Code"] = access_code_rows

        # Access code length validation.
        access_invalid = []
        for idx, v in df["Access Code"].items():
            if _is_missing(v):
                continue
            digits = _extract_digits(v)
            if digits and len(digits) < 4:
                access_invalid.append(idx)
                add_invalid_reason(idx, "Access Code", v, "Access Code length invalid")
        if access_invalid:
            invalid_cells["Access Code"] = sorted(set(invalid_cells.get("Access Code", []) + access_invalid))

    if "Account Code" in df.columns:
        if df["Account Code"].duplicated(keep=False).any():
            dup_idx = [idx for idx, dup in df["Account Code"].duplicated(keep=False).items() if dup]
            for idx in dup_idx:
                add_invalid_reason(idx, "Account Code", df.at[idx, "Account Code"], "Duplicate Account Code")
            invalid_cells["Account Code"] = sorted(set(invalid_cells.get("Account Code", []) + dup_idx))

        # Detect conflicting duplicates across identity fields.
        identity_cols = [c for c in ("full_name", "Address", "Email", "Cell Phone") if c in df.columns]
        if identity_cols and "Account Code" in df.columns:
            for idx in df.index:
                account_code = df.at[idx, "Account Code"]
                if _is_missing(account_code):
                    continue
                dup_mask = df["Account Code"].eq(account_code)
                if dup_mask.sum() <= 1:
                    continue
                for col in identity_cols:
                    if not df.loc[dup_mask, col].nunique(dropna=False) == 1:
                        invalid_cells.setdefault("Account Code", []).append(idx)
                        add_invalid_reason(idx, "Account Code", account_code, "Account Code duplicates with differing identity fields")
                        break

        if "full_name" in df.columns:
            for idx in df.index:
                full_name = df.at[idx, "full_name"]
                if _is_missing(full_name):
                    continue
                same_name = df["full_name"].eq(full_name)
                if same_name.sum() <= 1:
                    continue
                account_codes = df.loc[same_name, "Account Code"].dropna().unique()
                if len(account_codes) > 1:
                    invalid_cells.setdefault("Account Code", []).append(idx)
                    add_invalid_reason(idx, "Account Code", df.at[idx, "Account Code"], "Same full name with different Account Code")


    if "Width" in df.columns and "Length" in df.columns:
        # Default missing dimensions for occupied units, then compute Space Size.
        occupied_mask = (
            df["Status"].eq("Occupied") if "Status" in df.columns else pd.Series(False, index=df.index)
        )
        space_size_missing_mask = (
            df["Space Size"].apply(_is_missing) if "Space Size" in df.columns else pd.Series(True, index=df.index)
        )
        width_missing_mask = df["Width"].apply(_is_missing) & occupied_mask & space_size_missing_mask
        length_missing_mask = df["Length"].apply(_is_missing) & occupied_mask & space_size_missing_mask
        if width_missing_mask.any():
            df.loc[width_missing_mask, "Width"] = 1
        if length_missing_mask.any():
            df.loc[length_missing_mask, "Length"] = 1

        df["Space Size"] = df.apply(compute_space_size, axis=1)
        space_size_parsed_mask = (
            df["_space_size_parsed"] if "_space_size_parsed" in df.columns else pd.Series(False, index=df.index)
        )
        width_num = pd.to_numeric(df["Width"], errors="coerce")
        length_num = pd.to_numeric(df["Length"], errors="coerce")
        width_length_gt_one = (width_num > 1) & (length_num > 1)
        default_applied_mask = (
            (width_missing_mask | length_missing_mask)
            & df["Space Size"].notna()
            & ~space_size_parsed_mask
            & ~width_length_gt_one
        )
        space_size_rows = [idx for idx, applied in default_applied_mask.items() if applied]
        if space_size_rows:
            highlight_cells["yellow"]["Space Size"] = space_size_rows
            highlight_cells["yellow"]["Sq. Ft."] = space_size_rows


    if "State" in df.columns and "Country" in df.columns:
        # Set Country to United States when State is a valid US abbreviation.
        state_valid_mask = df["State"].apply(lambda x: is_valid_state_abbrev(x) if pd.notna(x) else False)
        country_missing_mask = df["Country"].apply(_is_missing)
        country_fill_mask = state_valid_mask & country_missing_mask
        df.loc[country_fill_mask, "Country"] = "United States"
        country_filled = [idx for idx, filled in country_fill_mask.items() if filled]
        if country_filled:
            highlight_cells["yellow"]["Country"] = country_filled
    
    if "Width" in df.columns and "Length" in df.columns:
        # Compute Sq. Ft. as Width * Length when both are present.
        df["Sq. Ft."] = df.apply(
            lambda row: row["Width"] * row["Length"]
            if pd.notna(row["Width"]) and pd.notna(row["Length"])
            else None,
            axis=1,
        )
    if "Paid Through Date" in df.columns:
        # Derive Bill Day from Paid Through Date.
        bill_day_missing = (
            df["Bill Day"].apply(_is_missing) if "Bill Day" in df.columns else pd.Series(True, index=df.index)
        )
        df["Bill Day"] = df["Paid Through Date"].apply(compute_bill_day)
        bill_day_filled = [
            idx
            for idx in df.index
            if bill_day_missing.get(idx, False) and not _is_missing(df.at[idx, "Bill Day"])
        ]
        if bill_day_filled:
            highlight_cells["yellow"]["Bill Day"] = bill_day_filled

    if "Bill Day" in df.columns:
        bill_day_invalid = []
        for idx, v in df["Bill Day"].items():
            if _is_missing(v):
                continue
            try:
                day_val = int(float(str(v).split(".")[0]))
            except Exception:
                bill_day_invalid.append(idx)
                add_invalid_reason(idx, "Bill Day", v, "Bill Day invalid")
                continue
            if not (1 <= day_val <= 31):
                bill_day_invalid.append(idx)
                add_invalid_reason(idx, "Bill Day", v, "Bill Day out of range")
        if bill_day_invalid:
            invalid_cells["Bill Day"] = sorted(set(invalid_cells.get("Bill Day", []) + bill_day_invalid))

    if mig_date and "Paid Through Date" in df.columns and "Bill Day" in df.columns:
        try:
            mig_date_obj = pd.to_datetime(mig_date, errors="coerce")
        except Exception:
            mig_date_obj = pd.NaT
        if pd.notna(mig_date_obj):
            last_day = (mig_date_obj + pd.offsets.MonthEnd(0)).date()
            ptd_invalid = []
            for idx, v in df["Paid Through Date"].items():
                if _is_missing(v):
                    continue
                ptd = pd.to_datetime(v, errors="coerce")
                if pd.isna(ptd):
                    ptd_invalid.append(idx)
                    add_invalid_reason(idx, "Paid Through Date", v, "Paid Through Date invalid")
                    continue
                original_ptd = ptd
                prepaid_col = "Prepaid Rent" if "Prepaid Rent" in df.columns else None
                if prepaid_col is None and "Prepaid Amount" in df.columns:
                    prepaid_col = "Prepaid Amount"

                if ptd.date() > last_day:
                    target_day = min(ptd.day, last_day.day)
                    try:
                        ptd = pd.Timestamp(year=mig_date_obj.year, month=mig_date_obj.month, day=target_day)
                    except Exception:
                        ptd = pd.Timestamp(year=mig_date_obj.year, month=mig_date_obj.month, day=last_day.day)
                    df.at[idx, "Paid Through Date"] = ptd.strftime("%m/%d/%y")
                    highlight_cells.setdefault("yellow", {}).setdefault("Paid Through Date", []).append(idx)

                    excess_days = (original_ptd.date() - ptd.date()).days
                    rent_val = df.at[idx, "Rent"] if "Rent" in df.columns else None
                    if excess_days > 0 and prepaid_col:
                        try:
                            rent_val_num = float(rent_val)
                        except Exception:
                            rent_val_num = None
                        if rent_val_num is not None:
                            days_in_month = original_ptd.days_in_month
                            daily_rate = rent_val_num / float(days_in_month)
                            excess_amount = round(daily_rate * excess_days, 2)
                            existing_prepaid = df.at[idx, prepaid_col]
                            try:
                                existing_val = float(existing_prepaid) if not _is_missing(existing_prepaid) else 0.0
                            except Exception:
                                existing_val = 0.0
                            df.at[idx, prepaid_col] = existing_val + excess_amount
                            highlight_cells.setdefault("yellow", {}).setdefault(prepaid_col, []).append(idx)
                        else:
                            add_invalid_reason(
                                idx,
                                prepaid_col,
                                rent_val,
                                "Prepaid not updated (Rent missing or invalid)",
                            )
                    elif excess_days > 0 and not prepaid_col:
                        add_invalid_reason(
                            idx,
                            "Paid Through Date",
                            v,
                            "Prepaid column not available for excess PTD",
                        )
                bill_day_val = df.at[idx, "Bill Day"]
                rent_balance = df.at[idx, "Rent Balance"] if "Rent Balance" in df.columns else None
                prepaid_rent = df.at[idx, prepaid_col] if prepaid_col else None

                if not _is_missing(bill_day_val):
                    try:
                        bill_day_num = int(float(str(bill_day_val)))
                    except Exception:
                        bill_day_num = None
                    if bill_day_num is not None and bill_day_num > 1:
                        expected_day = bill_day_num - 1
                        if ptd.day != expected_day:
                            ptd_invalid.append(idx)
                            add_invalid_reason(idx, "Paid Through Date", v, "Paid Through Date mismatches Bill Day")

                if ptd.date() > last_day and _is_missing(prepaid_rent):
                    ptd_invalid.append(idx)
                    add_invalid_reason(idx, "Paid Through Date", v, "PTD beyond migration month without Prepaid Rent")
                if ptd.date() < last_day and _is_missing(rent_balance):
                    ptd_invalid.append(idx)
                    add_invalid_reason(idx, "Paid Through Date", v, "PTD before migration month without Rent Balance")
                if ptd.date() == last_day and (not _is_missing(rent_balance) or not _is_missing(prepaid_rent)):
                    ptd_invalid.append(idx)
                    add_invalid_reason(idx, "Paid Through Date", v, "PTD equals migration month but balances present")
            if ptd_invalid:
                invalid_cells["Paid Through Date"] = sorted(
                    set(invalid_cells.get("Paid Through Date", []) + ptd_invalid)
                )

    if "Status" in df.columns and "Rent" in df.columns:
        rent_invalid = []
        for idx, v in df["Rent"].items():
            status_val = df.at[idx, "Status"]
            if status_val != "Occupied":
                continue
            if _is_missing(v):
                rent_invalid.append(idx)
                add_invalid_reason(idx, "Rent", v, "Rent missing for occupied unit")
                continue
            try:
                rent_val = float(v)
            except Exception:
                rent_invalid.append(idx)
                add_invalid_reason(idx, "Rent", v, "Rent invalid")
                continue
            if not (0.0 < rent_val < 10000.0):
                rent_invalid.append(idx)
                add_invalid_reason(idx, "Rent", v, "Rent out of range")
        if rent_invalid:
            invalid_cells["Rent"] = sorted(set(invalid_cells.get("Rent", []) + rent_invalid))

    if "Security Deposit" in df.columns and "Security Deposit Balance" in df.columns:
        sec_invalid = []
        for idx in df.index:
            dep = df.at[idx, "Security Deposit"]
            bal = df.at[idx, "Security Deposit Balance"]
            if _is_missing(dep) and _is_missing(bal):
                continue
            if not _is_missing(dep) and not _is_missing(bal):
                sec_invalid.append(idx)
                add_invalid_reason(idx, "Security Deposit", dep, "Security Deposit and Balance both present")
        if sec_invalid:
            invalid_cells["Security Deposit"] = sorted(set(invalid_cells.get("Security Deposit", []) + sec_invalid))
            invalid_cells["Security Deposit Balance"] = sorted(
                set(invalid_cells.get("Security Deposit Balance", []) + sec_invalid)
            )

    for col in ("Rent Balance", "Prepaid Rent"):
        if col not in df.columns:
            continue
        neg_invalid = []
        for idx, v in df[col].items():
            if _is_missing(v):
                continue
            try:
                if float(v) < 0:
                    neg_invalid.append(idx)
                    add_invalid_reason(idx, col, v, f"{col} is negative")
            except Exception:
                continue
        if neg_invalid:
            invalid_cells[col] = sorted(set(invalid_cells.get(col, []) + neg_invalid))

    if "_space_size_parsed" in df.columns:
        df = df.drop(columns=["_space_size_parsed"])

    return df, invalid_cells, highlight_cells, invalid_reasons
