from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import tempfile
import os
import re
from typing import Dict, List, Optional
from starlette.background import BackgroundTask

app = FastAPI(title="Mapping Normalization API")

# Allow local dev from the Next.js frontend.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _find_column(mapping_df: pd.DataFrame, expected_name: str) -> Optional[str]:
    """Case-insensitive helper to locate a column by expected name."""
    expected = expected_name.strip().lower()
    for col in mapping_df.columns:
        if col.strip().lower() == expected:
            return col
    return None


def _unique_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    result = []
    for val in values:
        if val not in seen:
            seen.add(val)
            result.append(val)
    return result


def parse_mapping(upload: UploadFile) -> List[dict]:
    """
    Read the mapping file into a normalized schema compatible with two formats:
    1) Legacy: columns source_col, output_col, default
    2) Migration context: Column Name in Spreadhseet Payload, Possible Variations,
       Central Maui Self Storage - Column Name, Central Maui Self Storage - Report Name (optional)
    """
    try:
        mapping_df = pd.read_excel(upload.file, engine="openpyxl")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to read mapping file: {exc}")

    columns_lower = {col.strip().lower() for col in mapping_df.columns}

    # Legacy mapping support
    if {"source_col", "output_col", "default"}.issubset(columns_lower):
        source_col = _find_column(mapping_df, "source_col")
        output_col = _find_column(mapping_df, "output_col")
        default_col = _find_column(mapping_df, "default")
        mapping_schema = []
        for _, row in mapping_df.iterrows():
            sources_raw = str(row[source_col]) if pd.notna(row[source_col]) else ""
            sources = [col.strip() for col in sources_raw.split(",") if col.strip()]
            if not sources:
                raise HTTPException(status_code=400, detail="Each mapping row needs at least one source_col")
            mapping_schema.append(
                {
                    "sources": sources,
                    "output": str(row[output_col]).strip(),
                    "default": "" if pd.isna(row[default_col]) else row[default_col],
                    "report": None,
                }
            )
        return mapping_schema

    # Migration mapping format (variations + optional report names)
    output_col_name = _find_column(mapping_df, "Column Name in Spreadhseet Payload")
    variations_col = _find_column(mapping_df, "Possible Variations")
    source_col_name = _find_column(mapping_df, "Central Maui Self Storage - Column Name")
    report_col_name = _find_column(mapping_df, "Central Maui Self Storage - Report Name")
    default_col = _find_column(mapping_df, "default")

    if not output_col_name or not variations_col:
        raise HTTPException(
            status_code=400,
            detail="Mapping file missing required columns. Expected either (source_col, output_col, default) "
            "or (Column Name in Spreadhseet Payload, Possible Variations).",
        )

    mapping_schema = []
    for _, row in mapping_df.iterrows():
        output = str(row[output_col_name]).strip() if pd.notna(row[output_col_name]) else ""
        if not output:
            continue

        sources: List[str] = []
        sources.append(output)

        if source_col_name and pd.notna(row.get(source_col_name, None)):
            candidate = str(row[source_col_name]).strip()
            if candidate:
                sources.append(candidate)

        variations_raw = str(row[variations_col]) if pd.notna(row[variations_col]) else ""
        if variations_raw:
            variations = (
                v.strip()
                for v in variations_raw.replace(";", ",").split(",")
                if v is not None
            )
            sources.extend([v for v in variations if v])

        default_value = ""
        if default_col and pd.notna(row.get(default_col, None)):
            default_value = row[default_col]

        report_name = None
        if report_col_name and pd.notna(row.get(report_col_name, None)):
            report_candidate = str(row[report_col_name]).strip()
            report_name = report_candidate if report_candidate else None

        mapping_schema.append(
            {
                "sources": _unique_preserve_order([s for s in sources if s]),
                "output": output,
                "default": default_value,
                "report": report_name,
            }
        )

    if not mapping_schema:
        raise HTTPException(status_code=400, detail="Mapping file contained no usable rows.")
    return mapping_schema


def parse_template(upload: UploadFile) -> List[str]:
    """
    Read template.xlsx to extract the desired output column order.
    """
    try:
        template_df = pd.read_excel(upload.file, engine="openpyxl")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to read template file: {exc}")

    if "output_col" not in template_df.columns:
        raise HTTPException(status_code=400, detail="Template file must contain 'output_col' column")

    template_order = [str(val).strip() for val in template_df["output_col"] if pd.notna(val)]
    if not template_order:
        raise HTTPException(status_code=400, detail="Template file contains no output_col values")
    return template_order


def make_unique(columns: List[str]) -> List[str]:
    """Ensure column names are unique by appending counters."""
    seen = {}
    unique_cols = []
    for col in columns:
        base = col
        if base not in seen:
            seen[base] = 0
            unique_cols.append(base)
        else:
            seen[base] += 1
            unique_cols.append(f"{base}_{seen[base]}")
    return unique_cols


def find_header_row(raw_df: pd.DataFrame, candidate_names: set) -> int:
    """
    Auto-detect header row inspired by context/excel_merger.py:
      - Scores rows based on non-null, string, uniqueness, presence of candidate mapping names,
        and common column-like words.
    """
    max_rows = min(25, len(raw_df))
    best_row = 0
    best_score = -1
    common_column_words = [
        "name",
        "date",
        "type",
        "status",
        "id",
        "number",
        "rent",
        "unit",
        "tenant",
        "balance",
        "paid",
        "total",
        "fee",
        "charge",
        "billing",
        "city",
        "age",
    ]

    for idx in range(max_rows):
        row = raw_df.iloc[idx]
        if row.isna().all():
            continue

        non_null_count = row.notna().sum()
        string_count = sum(isinstance(val, str) and len(str(val).strip()) > 0 for val in row)
        unique_count = len(row.dropna().unique())
        numeric_count = sum(isinstance(val, (int, float)) and not pd.isna(val) for val in row)

        # Column-like detection
        column_like_count = 0
        for val in row:
            if isinstance(val, str):
                val_lower = val.lower().strip()
                if len(val_lower) < 50 and any(word in val_lower for word in common_column_words):
                    column_like_count += 1
                elif len(val_lower.split()) <= 4 and len(val_lower) < 50:
                    column_like_count += 0.5

        # Candidate mapping name bonus
        row_vals = {str(v).strip().lower() for v in row if pd.notna(v) and str(v).strip()}
        candidate_overlap = len(row_vals & candidate_names)

        if non_null_count < 1:
            continue

        score = (string_count * 3) + (unique_count * 2) + non_null_count
        score += column_like_count * 5
        score += candidate_overlap * 15  # strong signal if mapping names appear

        if string_count >= non_null_count * 0.8:
            score += 10
        if unique_count >= non_null_count * 0.9:
            score += 15
        if numeric_count > string_count:
            score -= 10
        if non_null_count <= 2:
            score -= 20

        if score > best_score:
            best_score = score
            best_row = idx

    return best_row


def clean_dataframe(raw_df: pd.DataFrame, mapping_schema: List[dict]) -> pd.DataFrame:
    """
    Clean messy inputs where headers or metadata occupy top rows.
    Strategy:
      - Read with header=None.
      - Detect the header row using scoring informed by mapping names and column-like heuristics.
      - Use that row as header and drop rows above it.
      - Drop rows/columns that are entirely empty after header assignment.
    """
    # Build set of candidate header tokens from mapping sources and outputs.
    candidate_names = set()
    for rule in mapping_schema:
        candidate_names.update([src.lower() for src in rule["sources"]])
        candidate_names.add(rule["output"].lower())

    header_index = find_header_row(raw_df, candidate_names)

    # Set header and data
    header_row = raw_df.iloc[header_index].fillna("").astype(str).str.strip().tolist()
    header_row = [h if h else "unnamed" for h in header_row]
    header_row = make_unique(header_row)

    data_df = raw_df.iloc[header_index + 1 :].copy()
    data_df.columns = header_row[: len(data_df.columns)]
    data_df = data_df.reset_index(drop=True)

    # Drop rows that are entirely empty/blank after header assignment.
    def row_is_empty(row):
        return all((pd.isna(v) or str(v).strip() == "") for v in row)

    data_df = data_df[~data_df.apply(row_is_empty, axis=1)]
    data_df = data_df.reset_index(drop=True)

    # Drop columns that are entirely empty/NaN/blank
    def is_empty_series(s: pd.Series) -> bool:
        return s.isna().all() or all(str(v).strip() == "" for v in s)

    data_df = data_df[[col for col in data_df.columns if not is_empty_series(data_df[col])]]
    return data_df


def read_input_file(upload: UploadFile, mapping_schema: List[dict]) -> pd.DataFrame:
    """
    Read an uploaded CSV or Excel file into a cleaned DataFrame.
    Handles messy headers by scanning for the likely header row.
    """
    filename = upload.filename.lower()
    try:
        if filename.endswith(".csv"):
            raw_df = pd.read_csv(upload.file, header=None)
        elif filename.endswith(".xls"):
            raw_df = pd.read_excel(upload.file, engine="xlrd", header=None)
        else:
            raw_df = pd.read_excel(upload.file, engine="openpyxl", header=None)
        return clean_dataframe(raw_df, mapping_schema)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to read input file {upload.filename}: {exc}")


def transform_rows(mapping_schema: List[dict], dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Apply mapping rules across all provided DataFrames and return one unified DataFrame.
    """
    transformed_rows = []

    for df in dataframes:
        # Normalize column names for easier matching
        normalized_cols = {col.lower(): col for col in df.columns}

        for _, row in df.iterrows():
            output_row = {}
            for rule in mapping_schema:
                value = None
                for source in rule["sources"]:
                    # Match case-insensitive to be forgiving
                    source_key = source.lower().strip()
                    if source_key in normalized_cols:
                        raw_val = row[normalized_cols[source_key]]
                        if pd.notna(raw_val):
                            value = raw_val
                            break
                if value is None:
                    value = rule.get("default", "")
                output_row[rule["output"]] = value
            transformed_rows.append(output_row)

    # If there were no rows at all, return an empty DataFrame with mapping outputs as columns.
    if not transformed_rows:
        return pd.DataFrame(columns=[rule["output"] for rule in mapping_schema])

    return pd.DataFrame(transformed_rows)


def apply_template_order(df: pd.DataFrame, template_order: Optional[List[str]], mapping_schema: List[dict]) -> pd.DataFrame:
    """
    Reorder DataFrame columns according to template_order if provided.
    Behavior: columns not in the template are appended at the end (documented choice).
    If no template is provided, the order follows mapping_schema output order.
    """
    if template_order:
        # Ensure all template columns exist; fill missing with empty string.
        for col in template_order:
            if col not in df.columns:
                df[col] = ""

        # Reindex to template order first
        ordered_cols = template_order.copy()
        # Append any extra mapped columns not present in template to preserve data.
        extras = [col for col in df.columns if col not in ordered_cols]
        ordered_cols.extend(extras)
        return df.reindex(columns=ordered_cols)

    # No template: follow mapping order as the default ordering policy.
    mapping_order = [rule["output"] for rule in mapping_schema]
    extras = [col for col in df.columns if col not in mapping_order]
    ordered_cols = mapping_order + extras
    for col in ordered_cols:
        if col not in df.columns:
            df[col] = ""
    return df.reindex(columns=ordered_cols)


def standardize_date_format(value, output_format: str = "%Y-%m-%d") -> str:
    """Best-effort date normalization to a consistent string."""
    if pd.isna(value) or value == "" or str(value).strip() == "" or str(value).lower() == "none":
        return ""

    date_formats = [
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%m.%d.%Y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%y",
        "%d/%m/%y",
        "%b %d, %Y",
        "%B %d, %Y",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
    ]

    for fmt in date_formats:
        try:
            dt = pd.to_datetime(value, format=fmt)
            return dt.strftime(output_format)
        except Exception:
            continue

    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.notna(dt):
            return dt.strftime(output_format)
    except Exception:
        pass

    try:
        if isinstance(value, (int, float)) and 1 <= value <= 100000:
            dt = pd.to_datetime("1899-12-30") + pd.Timedelta(days=value)
            return dt.strftime(output_format)
    except Exception:
        pass

    return str(value)


def clean_currency(value) -> float:
    """Remove currency symbols/commas and coerce to float."""
    if pd.isna(value) or value == "" or str(value).strip() == "":
        return 0.0
    try:
        cleaned = (
            str(value)
            .replace("$", "")
            .replace(",", "")
            .replace("(", "-")
            .replace(")", "")
            .strip()
        )
        return float(cleaned)
    except Exception:
        return 0.0


def normalize_column_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).strip().lower())


def determine_report_for_file(mapping_schema: List[dict], filename: str) -> Optional[str]:
    """Try to pick a report name based on the filename when report-specific mappings exist."""
    reports = [rule["report"] for rule in mapping_schema if rule.get("report")]
    if not reports:
        return None
    fname = filename.lower()
    for report in reports:
        if report and report.lower() in fname:
            return report
    return None


def map_dataframe_to_outputs(df: pd.DataFrame, mapping_schema: List[dict], filename: str) -> pd.DataFrame:
    """
    Rename/map columns to the standardized output columns using the parsed mapping schema.
    Falls back to defaults when a source column is missing.
    """
    selected_report = determine_report_for_file(mapping_schema, filename)
    rules = [
        rule
        for rule in mapping_schema
        if not rule.get("report") or rule.get("report") == selected_report
    ]
    if not rules:
        rules = mapping_schema

    normalized_cols = {normalize_column_name(col): col for col in df.columns}
    mapped_columns: Dict[str, pd.Series] = {}

    for rule in rules:
        output = rule["output"]
        for source in rule["sources"]:
            key = normalize_column_name(source)
            if key in normalized_cols:
                mapped_columns[output] = df[normalized_cols[key]]
                break
        if output not in mapped_columns:
            default_value = "" if pd.isna(rule.get("default", "")) else rule.get("default", "")
            mapped_columns[output] = pd.Series([default_value] * len(df))

    return pd.DataFrame(mapped_columns)


def detect_merge_key(dataframes: List[pd.DataFrame], template_order: Optional[List[str]]) -> str:
    """
    Choose a merge key. Prefer 'Space' (common in the provided mapping). Otherwise
    use the first template column, or the first column of the first dataframe.
    """
    if any("space" == col.lower().strip() for df in dataframes for col in df.columns):
        return "Space"
    if template_order:
        return template_order[0]
    return dataframes[0].columns[0]


def merge_mapped_dataframes(dataframes: List[pd.DataFrame], merge_key: str) -> pd.DataFrame:
    """Outer-merge mapped dataframes on the merge key, combining duplicate columns."""
    if not dataframes:
        return pd.DataFrame()

    prepared = []
    for df in dataframes:
        df_copy = df.copy()
        if merge_key not in df_copy.columns:
            df_copy[merge_key] = ""
        prepared.append(df_copy)

    merged = prepared[0]
    for df in prepared[1:]:
        overlap = set(merged.columns) & set(df.columns) - {merge_key}
        merged = merged.merge(df, on=merge_key, how="outer", suffixes=("", "_dup"))
        for col in overlap:
            dup_col = f"{col}_dup"
            if dup_col in merged.columns:
                try:
                    merged[col] = merged[col].combine_first(merged[dup_col])
                except Exception:
                    merged[col] = merged[col].fillna(merged[dup_col])
                merged = merged.drop(columns=[dup_col])
    return merged


def coerce_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply light type normalization so numeric/date/boolean/text columns are consistent
    and missing values are filled sensibly.
    """
    numeric_keywords = [
        "amount",
        "balance",
        "rate",
        "rent",
        "fee",
        "charge",
        "price",
        "cost",
        "deposit",
        "payment",
        "value",
        "premium",
        "discount",
        "tax",
        "coverage",
        "total",
    ]
    date_keywords = [
        "date",
        "time",
        "day",
        "paid",
        "expiration",
        "effective",
        "start",
        "end",
        "move",
        "through",
        "thru",
    ]
    bool_keywords = [
        "flag",
        "enabled",
        "access",
        "paperless",
        "autopay",
        "offline",
        "business",
    ]
    id_keywords = ["id", "number", "code", "#", "serial", "policy", "lease"]

    df_processed = df.copy()
    for col in df_processed.columns:
        col_lower = col.lower()
        series = df_processed[col]

        if any(keyword in col_lower for keyword in numeric_keywords):
            df_processed[col] = series.apply(clean_currency).fillna(0)
            continue

        if any(keyword in col_lower for keyword in date_keywords):
            df_processed[col] = series.apply(
                lambda x: standardize_date_format(x) if pd.notna(x) and str(x).strip() else ""
            )
            continue

        if any(keyword in col_lower for keyword in bool_keywords):
            truthy = {"yes", "y", "true", "1", "t"}
            falsy = {"no", "n", "false", "0", "f"}

            def to_bool(val):
                if isinstance(val, bool):
                    return val
                if pd.isna(val):
                    return False
                sval = str(val).strip().lower()
                if sval in truthy:
                    return True
                if sval in falsy:
                    return False
                return bool(val)

            df_processed[col] = series.apply(to_bool)
            continue

        if any(keyword in col_lower for keyword in id_keywords):
            df_processed[col] = series.astype(str).replace("nan", "").fillna("")
            continue

        df_processed[col] = series.apply(lambda x: "" if pd.isna(x) else x)

    return df_processed


@app.post("/process")
async def process_files(
    mapping: UploadFile = File(...),
    files: List[UploadFile] = File(...),
    template: Optional[UploadFile] = File(None),
):
    if not files:
        raise HTTPException(status_code=400, detail="At least one data file is required.")

    mapping_schema = parse_mapping(mapping)
    template_order = parse_template(template) if template else None

    cleaned_frames = []
    for upload in files:
        cleaned_df = read_input_file(upload, mapping_schema)
        mapped_df = map_dataframe_to_outputs(cleaned_df, mapping_schema, upload.filename)
        cleaned_frames.append(mapped_df)

    merge_key = detect_merge_key(cleaned_frames, template_order)
    merged_df = merge_mapped_dataframes(cleaned_frames, merge_key)
    merged_df = apply_template_order(merged_df, template_order, mapping_schema)
    final_df = coerce_column_types(merged_df)

    # Write to a temporary Excel file and return it.
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp_path = tmp.name
        final_df.to_excel(tmp_path, index=False, engine="openpyxl")

    filename = "final_output.xlsx"
    # FileResponse handles opening the file; we use os.remove in background cleanup.
    # BackgroundTask cleans up the temp file after the response is sent.
    background_task = BackgroundTask(lambda: os.path.exists(tmp_path) and os.remove(tmp_path))
    return FileResponse(
        path=tmp_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
        background=background_task,
    )
