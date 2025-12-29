"""
Validation and normalization utilities for merged Excel data.

This module is pure: it operates only on the provided DataFrame and
does not perform any file I/O.
"""

from __future__ import annotations
import re
from decimal import Decimal, ROUND_HALF_UP
import random
from typing import Dict, List, Tuple
import pandas as pd # type: ignore

# ---------------------------------------------------------------------------
# Cleaning helpers
# ---------------------------------------------------------------------------

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
    """Normalize boolean-like fields."""
    if pd.isna(val):
        return False
    return str(val).strip().lower() in {"y", "yes", "true", "1"}


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


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------

_NAME_ALLOWED_PATTERN = re.compile(r"^[A-Za-z\s,'-]+$")
_NAME_TOKEN_PATTERN = re.compile(r"^[A-Za-z]+(?:[\'-][A-Za-z]+)*$")
_NAME_COMPANY_PATTERN = re.compile(
    r"\b(c\/o|care of|company|co|inc|inc\.|llc|l\.l\.c\.|corp|corp\.|ltd|ltd\.)\b",
    re.IGNORECASE,
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
        return str(val).strip() == ""
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
    r"^\s*(\d+(?:\.\d+)?)\s*[xX]\s*(\d+(?:\.\d+)?)(?:\s*[xX]\s*(\d+(?:\.\d+)?))?(?:\s*[A-Za-z].*)?$"
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

ZIP_COLS = {"ZIP" , "Alt ZIP"}
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
    df: pd.DataFrame, mapping_path: str
) -> Tuple[pd.DataFrame, Dict[str, List[int]], Dict[str, Dict[str, List[int]]], List[Dict[str, object]]]:
    """
    Accepts a merged DataFrame
    Applies mapping-driven defaults, validation, normalization, and derived column logic
    Returns:
        - cleaned DataFrame
        - invalid_cells: dict[column_name] -> list[row_index]
        - highlight_cells: dict[color] -> dict[column_name] -> list[row_index] (for informational highlighting)
    """
    df = apply_default_values_from_mapping(df, mapping_path)
    df = parse_space_category(df)
    df = parse_space_size(df)
    df = parse_space_type_dimensions(df)
    df = df.copy()
    invalid_cells: Dict[str, List[int]] = {}
    highlight_cells: Dict[str, Dict[str, List[int]]] = {"red": {}, "blue": {}}
    invalid_reasons: List[Dict[str, object]] = []

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

        
        # elif col in BOOLEAN_COLS:
        #     df[col] = df[col].apply(clean_boolean)

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
                if pd.isna(v) or str(v).strip() == "":
                    col_values.append(None)
                    continue
                norm = normalize_state(v)
                if norm:
                    col_values.append(norm)
                else:
                    col_values.append(v)
                    invalid_idx.append(idx)
                    add_invalid_reason(idx, col, v, "Invalid state")
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
                if pd.isna(v) or str(v).strip() == "":
                    col_values.append(None)
                    continue

                s = str(v).strip()
                if s.endswith(".0"):
                    s = s[:-2]

                if re.fullmatch(r"\d{5}", s):
                    col_values.append(clean_zip(s))
                else:
                    col_values.append(v)
                    invalid_idx.append(idx)
                    add_invalid_reason(idx, col, v, "Invalid ZIP (expected 5 digits)")
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

    if "Move In Date" in df.columns and "Paid Date" in df.columns:
        # Fill missing Move In Date with Paid Date when available.
        missing_move_in_mask = df["Move In Date"].apply(_is_missing)
        paid_present_mask = ~df["Paid Date"].apply(_is_missing)
        df.loc[missing_move_in_mask & paid_present_mask, "Move In Date"] = df.loc[
            missing_move_in_mask & paid_present_mask, "Paid Date"
        ]

    if "First Name" in df.columns and "Last Name" in df.columns:
        # Derive Status from presence of occupant name.
        df["Status"] = df.apply(
            lambda row: "Occupied"
            if (pd.notna(row["First Name"]) and str(row["First Name"]).strip() != "")
            or (pd.notna(row["Last Name"]) and str(row["Last Name"]).strip() != "")
            else "Vacant",
            axis=1,
        )

    
    if "Status" in df.columns:
        if "Paid Date" in df.columns:
            # Paid through date = paid date + 1 month - 1 day
            def _calc_paid_through(row):
                if row.get("Status") != "Occupied":
                    return row.get("Paid Through Date")
                if not _is_missing(row.get("Paid Through Date")):
                    return row.get("Paid Through Date")
                paid_date = pd.to_datetime(row.get("Paid Date"), errors="coerce")
                if pd.isna(paid_date):
                    return None
                paid_through = paid_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)
                return paid_through.strftime("%m/%d/%y")

            df["Paid Through Date"] = df.apply(_calc_paid_through, axis=1)
            
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

    if "Access Code" not in df.columns:
        # Ensure Access Code column exists for derivation.
        df["Access Code"] = None

    used_access_codes: set[str] = set()
    if "Access Code" in df.columns:
        # Track existing access codes to avoid duplicates.
        for val in df["Access Code"]:
            digits = _extract_digits(val)
            last4 = _last4(digits)
            if last4:
                used_access_codes.add(last4)

    if "Access Code" in df.columns:
        # Populate missing access codes for occupied units (phone last4 or unique random).
        access_code_rows: List[int] = []
        for idx in df.index:
            # Only derive/populate for occupied units.
            status_val = df.at[idx, "Status"] if "Status" in df.columns else None
            if status_val != "Occupied":
                continue

            current_access = df.at[idx, "Access Code"]
            if not _is_missing(current_access):
                continue  # Leave existing Access Code untouched.

            cell_digits = _extract_digits(df.at[idx, "Cell Phone"]) if "Cell Phone" in df.columns else None
            alt_digits = _extract_digits(df.at[idx, "Alt Cell Phone"]) if "Alt Cell Phone" in df.columns else None

            access_code_val = _last4(cell_digits) or _last4(alt_digits)
            if access_code_val is None:
                access_code_val = _generate_unique_access_code(used_access_codes)
            try:
                df.at[idx, "Access Code"] = int(access_code_val)
                df.at[idx,"Account Code"] = int(access_code_val)
            except Exception:
                df.at[idx, "Access Code"] = access_code_val
                df.at[idx,"Account Code"] = access_code_val
            used_access_codes.add(access_code_val)  
            access_code_rows.append(idx)

        if access_code_rows:
            highlight_cells["blue"]["Access Code"] = access_code_rows
            highlight_cells["blue"]["Account Code"] = access_code_rows


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
            highlight_cells["red"]["Space Size"] = space_size_rows
            highlight_cells["red"]["Sq. Ft."] = space_size_rows


    if "State" in df.columns and "Country" in df.columns:
        # Set Country to United States when State is a valid US abbreviation.
        df.loc[df["State"].apply(lambda x: is_valid_state_abbrev(x) if pd.notna(x) else False), "Country"] = "United States"
    
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
        df["Bill Day"] = df["Paid Through Date"].apply(compute_bill_day)

    if "_space_size_parsed" in df.columns:
        df = df.drop(columns=["_space_size_parsed"])

    return df, invalid_cells, highlight_cells, invalid_reasons
