from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import tempfile
import os
from typing import List, Optional
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


def parse_mapping(upload: UploadFile) -> List[dict]:
    """
    Read mapping.xlsx into a normalized schema:
    [
      {"sources": ["id", "id_number"], "output": "id", "default": ""},
      ...
    ]
    """
    try:
        mapping_df = pd.read_excel(upload.file, engine="openpyxl")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to read mapping file: {exc}")

    required_columns = {"source_col", "output_col", "default"}
    if not required_columns.issubset(mapping_df.columns):
        missing = required_columns - set(mapping_df.columns)
        raise HTTPException(
            status_code=400,
            detail=f"Mapping file missing required columns: {', '.join(missing)}",
        )

    mapping_schema = []
    for _, row in mapping_df.iterrows():
        sources_raw = str(row["source_col"]) if pd.notna(row["source_col"]) else ""
        sources = [col.strip() for col in sources_raw.split(",") if col.strip()]
        if not sources:
            raise HTTPException(status_code=400, detail="Each mapping row needs at least one source_col")

        mapping_schema.append(
            {
                "sources": sources,
                "output": str(row["output_col"]).strip(),
                "default": "" if pd.isna(row["default"]) else row["default"],
            }
        )
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


def read_input_file(upload: UploadFile) -> pd.DataFrame:
    """
    Read an uploaded CSV or Excel file into a DataFrame.
    """
    filename = upload.filename.lower()
    try:
        if filename.endswith(".csv"):
            return pd.read_csv(upload.file)
        else:
            return pd.read_excel(upload.file, engine="openpyxl")
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
                    source_key = source.lower()
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

    dataframes = [read_input_file(upload) for upload in files]
    final_df = transform_rows(mapping_schema, dataframes)
    final_df = apply_template_order(final_df, template_order, mapping_schema)

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
