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

import pandas as pd

# ---------------------------------------------------------------------------
# Cleaning helpers
# ---------------------------------------------------------------------------


def clean_currency(val):
    """
    Normalize currency:
    - Removes $, commas, spaces
    - Uses '-' sign for negatives (parentheses are ignored)
    - Returns int when value is whole dollars, otherwise float with 2 decimals
    """
    if pd.isna(val):
        return None

    raw = str(val).strip()
    negative = "-" in raw

    s = re.sub(r"[^0-9.]", "", raw)
    if not s:
        return None

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
) -> Tuple[pd.DataFrame, Dict[str, List[int]], Dict[str, Dict[str, List[int]]]]:
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
    df = df.copy()
    invalid_cells: Dict[str, List[int]] = {}
    highlight_cells: Dict[str, Dict[str, List[int]]] = {"red": {}, "blue": {}}

    for col in df.columns:
        if col in PHONE_COLS:
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
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values
        

        elif col in CURRENCY_COLS:
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
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

        elif col in NUMBER_COLS:
            col_values = []
            invalid_idx = []
            for idx, v in df[col].items():
                if pd.isna(v) or str(v).strip() == "":
                    col_values.append(None)
                    continue
                cleaned = clean_number(v)
                if cleaned is not None:
                    col_values.append(cleaned)
                else:
                    col_values.append(v)
                    invalid_idx.append(idx)
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

        elif col in DATE_COLS:
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
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

        
        # elif col in BOOLEAN_COLS:
        #     df[col] = df[col].apply(clean_boolean)

        elif col in EMAIL_COLS:
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
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

        elif col in STATE_COLS:
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
            if invalid_idx:
                invalid_cells[col] = invalid_idx
            df[col] = col_values

    if "First Name" in df.columns and "Last Name" in df.columns:
        df["Status"] = df.apply(
            lambda row: "Occupied"
            if (pd.notna(row["First Name"]) and str(row["First Name"]).strip() != "")
            or (pd.notna(row["Last Name"]) and str(row["Last Name"]).strip() != "")
            else "Vacant",
            axis=1,
        )

    # -----------------------------------------------------------------------
    # Access Code resolution (derivation/generation + marking)
    # -----------------------------------------------------------------------
    def _extract_digits(val: object) -> str | None:
        if pd.isna(val):
            return None
        digits = re.sub(r"\D", "", str(val))
        return digits if digits else None

    def _is_valid_phone_digits(digits: str | None) -> bool:
        if not digits:
            return False
        if len(digits) < 10:
            return False
        last10 = digits[-10:]
        return last10 != "0000000000"

    def _last4(digits: str | None) -> str | None:
        if not digits or len(digits) < 4:
            return None
        return digits[-4:]

    def _generate_unique_phone(existing: set[str]) -> str:
        for _ in range(10000):
            num = random.randint(2000000000, 9999999999)  # 10 digits, first digit >=2
            digits = f"{num:010d}"
            if digits.startswith("555"):
                continue
            if digits in existing:
                continue
            existing.add(digits)
            return digits
        raise RuntimeError("Failed to generate a unique phone number.")

    if "Access Code" not in df.columns:
        df["Access Code"] = None

    used_phone_numbers: set[str] = set()
    for phone_col in ("Cell Phone", "Alt Cell Phone"):
        if phone_col in df.columns:
            for val in df[phone_col]:
                digits = _extract_digits(val)
                if _is_valid_phone_digits(digits):
                    used_phone_numbers.add(digits[-10:])

    if "Access Code" in df.columns:
        access_code_rows: List[int] = []
        generated_phone_rows: List[int] = []
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

            selected_digits = None
            derived_from_phone = False
            if _is_valid_phone_digits(cell_digits):
                selected_digits = cell_digits
                derived_from_phone = True
            elif _is_valid_phone_digits(alt_digits):
                selected_digits = alt_digits
                derived_from_phone = True
            else:
                # Generate a new valid, unique phone number and assign to Cell Phone.
                generated_digits = _generate_unique_phone(used_phone_numbers)
                df.at[idx, "Cell Phone"] = int(generated_digits)
                selected_digits = generated_digits
                generated_phone_rows.append(idx)
                derived_from_phone = True

            access_code_val = _last4(selected_digits)
            if access_code_val is not None and derived_from_phone:
                try:
                    df.at[idx, "Access Code"] = int(access_code_val)
                except Exception:
                    df.at[idx, "Access Code"] = access_code_val
                access_code_rows.append(idx)

        if access_code_rows:
            highlight_cells["blue"]["Access Code"] = access_code_rows
        if generated_phone_rows:
            highlight_cells["blue"]["Cell Phone"] = generated_phone_rows

    if "Width" in df.columns and "Length" in df.columns:
        occupied_mask = (
            df["Status"].eq("Occupied") if "Status" in df.columns else pd.Series(False, index=df.index)
        )
        width_missing_mask = df["Width"].apply(_is_missing) & occupied_mask
        length_missing_mask = df["Length"].apply(_is_missing) & occupied_mask
        if width_missing_mask.any():
            df.loc[width_missing_mask, "Width"] = 1
        if length_missing_mask.any():
            df.loc[length_missing_mask, "Length"] = 1

        df["Space Size"] = df.apply(compute_space_size, axis=1)
        default_applied_mask = (width_missing_mask | length_missing_mask) & df["Space Size"].notna()
        space_size_rows = [idx for idx, applied in default_applied_mask.items() if applied]
        if space_size_rows:
            highlight_cells["red"]["Space Size"] = space_size_rows
            highlight_cells["red"]["Sq. Ft."] = space_size_rows


    if "State" in df.columns and "Country" in df.columns:
        df.loc[df["State"].apply(lambda x: is_valid_state_abbrev(x) if pd.notna(x) else False), "Country"] = "USA"
    
    if "Width" in df.columns and "Length" in df.columns:
        df["Sq. Ft."] = df.apply(
            lambda row: row["Width"] * row["Length"]
            if pd.notna(row["Width"]) and pd.notna(row["Length"])
            else None,
            axis=1,
        )
    if "Paid Through Date" in df.columns:
        df["Bill Day"] = df["Paid Through Date"].apply(compute_bill_day)

    return df, invalid_cells, highlight_cells
