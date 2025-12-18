# Excel Mapping & Template Normalizer

This project provides a FastAPI backend and a Next.js frontend to normalize multiple CSV/Excel files into a single standardized `final_output.xlsx` using:
- `mapping.xlsx` to define how input columns map to standardized columns and which defaults to use.
- Optional `template.xlsx` to define the final column order.

Docker Compose runs both services together for a ready-to-use experience.

---

## How the mapping works
- `mapping.xlsx` columns:
  - `source_col`: one or more input column names, comma-separated (e.g., `id,id_number`).
  - `output_col`: the standardized column name in the output.
  - `default`: value to use if none of the sources are present or they are NaN.
- For each data row:
  1) Iterate through `sources` in order (case-insensitive match to input columns).
  2) Pick the first present/non-NaN value.
  3) If none found, use `default`.
- Multiple input files are processed with the same mapping; all transformed rows are merged.

### Template ordering
- If `template.xlsx` is provided, its `output_col` column defines the column order. Missing template columns are created with empty strings. Extra mapped columns not listed in the template are **appended at the end** (chosen behavior).
- If no template is provided, columns follow mapping order, then any extras.

---

## Example
`mapping.xlsx`
```
source_col        | output_col | default
id,id_number      | id         |
full_name,name    | name       | N/A
age               | age        |
city              | city       | Unknown
```

`template.xlsx`
```
output_col
id
name
age
city
```

Input files:
- file1 columns: `id_number, full_name, age`
- file2 columns: `id, name, city`

Output:
```
id | name  | age | city
1  | Alice | 23  | Unknown
2  | Bob   | N/A | LA
```

---

## Quick start (Docker)
1) Build and run:
```bash
docker-compose up --build
```
2) Frontend: http://localhost:3000  
   - Upload `mapping.xlsx` (required), `template.xlsx` (optional), and one or more data files (CSV/XLSX).  
   - Click “Process Files” to download `final_output.xlsx`.
3) Backend (for reference/testing): http://localhost:8000/docs

---

## Local backend only (optional)
```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```
Then point the frontend to `http://localhost:8000` (set `NEXT_PUBLIC_BACKEND_URL`).

---

## API request example (curl)
```bash
curl -X POST http://localhost:8000/process \
  -F "mapping=@mapping.xlsx" \
  -F "template=@template.xlsx" \
  -F "files=@data1.xlsx" \
  -F "files=@data2.csv" \
  --output final_output.xlsx
```

---

## Dummy data generator (Python)
Use this script to create sample files that match the **strict mapping format** (with optional default-only rows).

`scripts/generate_dummy_files.py`
```python
import pandas as pd
from pathlib import Path

def main(out_dir="samples"):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Mapping format: output_col, report_name, column_name, possible_variations, default_value (optional)
    # report_name must match the uploaded filename (normalized: lowercase, no extension).
    mapping = pd.DataFrame([
        {"output_col": "id",    "report_name": "people",   "column_name": "id",            "possible_variations": "identifier", "default_value": ""},
        {"output_col": "name",  "report_name": "people",   "column_name": "name",          "possible_variations": "full name",  "default_value": ""},
        {"output_col": "email", "report_name": "contacts", "column_name": "email",         "possible_variations": "e-mail",    "default_value": ""},
        {"output_col": "phone", "report_name": "contacts", "column_name": "phone number",  "possible_variations": "phn_no,phone", "default_value": ""},
        # Example default-only row (no report/column needed)
        {"output_col": "status", "report_name": "", "column_name": "", "possible_variations": "", "default_value": "active"},
    ])
    mapping.to_excel(out / "mapping.xlsx", index=False)

    # Optional template controls column order in the output
    template = pd.DataFrame({"output_col": ["id", "name", "email", "phone"]})
    template.to_excel(out / "template.xlsx", index=False)

    # Data files. Filenames (without extensions) should match report_name above.
    people = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "identifier": [1, 2, 3],  # variation of id
    })
    people.to_excel(out / "people.xlsx", index=False)

    contacts = pd.DataFrame({
        "id": [1, 3],
        "email": ["alice@example.com", "carol@example.com"],
        "phn_no": ["111-222-3333", "333-444-5555"],  # variation of phone number
    })
    contacts.to_csv(out / "contacts.csv", index=False)

    print(f"Dummy files written to {out.resolve()}")

if __name__ == "__main__":
    main()
```

Run:
```bash
python scripts/generate_dummy_files.py
```
Then upload `mapping.xlsx`, optional `template.xlsx`, and the generated `people.xlsx` and `contacts.csv` from the `samples/` directory.

---

## Project structure
```
/backend         FastAPI service (mapping/template processing)
  main.py
  requirements.txt
  Dockerfile
/frontend        Next.js UI for uploads/download
  app/page.jsx
  app/globals.css
  components/ui/...
  package.json
  Dockerfile
docker-compose.yml
```

---

## How this app works (end-to-end)
- Uploads: frontend collects `mapping.xlsx` (required), optional `template.xlsx`, and one or more data files (CSV/XLS/XLSX).
- Mapping parse: backend reads the strict mapping format (`output_col`, `report_name`, `column_name`, `possible_variations`). `report_name` must match the uploaded filename (normalized: lowercase, no extension). Variations are treated as synonyms for fuzzy column resolution.
- Header cleaning: each input file is read without assuming a header. We auto-detect the true header row using heuristics and mapping tokens, then drop metadata and empty rows/cols.
- Column resolution: per mapping row, we pick the best matching source column (exact, normalized, variation, fuzzy contains).
- Row resolution: we detect a join key across reports (e.g., `id`, `email`, `space`) and use it to assemble the correct entity row from each report. If no key is found, we fall back to row-position alignment.
- Output build: for each `output_col`, we pull the value from the specified `report_name`/column; if the join key is present, we match the row on that key.
- Template/order: if a template is supplied, missing template columns are added and order is enforced; extra mapped columns are appended.
- Type handling: values are now passed through as-is (no coercion) to avoid unintended boolean/numeric conversions.
- Output: a single `final_output.xlsx` is streamed back to the browser.

### Flow chart (high level)
```
User uploads files
       |
       v
Frontend builds FormData --> POST /process
       |
       v
Backend parses mapping & optional template
       |
       v
For each input file:
  - Read (CSV/XLS/XLSX) with header detection
  - Clean rows/columns
  - Resolve columns using mapping (base + variations)
       |
       v
Detect join key across reports (id/email/space/etc.)
       |
       v
Build output rows:
  - For each entity key: pull mapped column from target report
  - Fallback: align rows by position if no key
Apply template ordering
       |
       v
Write temp Excel & respond as download
```

### Architecture at a glance
- **Frontend (Next.js)**: `frontend/app/page.jsx` UI for file selection and submission; environment var `NEXT_PUBLIC_BACKEND_URL` points to FastAPI.
- **Backend (FastAPI)**: single `/process` endpoint in `backend/main.py` handling mapping/template parsing, header detection, column resolution (with variations/fuzzy), join-key detection, row-level assembly, ordering, and Excel export.
- **Data helpers**: header detection, synonym/fuzzy matching, join-key detection, and pass-through type handling live alongside the endpoint in `backend/main.py`.
- **Containerization**: `docker-compose.yml` runs both services; each service has its own Dockerfile.

---

## Data flow (expanded)

```
Client
  -> Upload mapping.xlsx (output_col, report_name, column_name, possible_variations)
  -> Upload optional template.xlsx (output_col list)
  -> Upload one or more data files (CSV/XLS/XLSX)

Backend /process
  1) Parse mapping rules (strict schema)
  2) Parse template (optional ordering)
  3) For each file:
       - Auto-detect header row
       - Clean metadata/empty rows/cols
       - Store dataframe under normalized report key (filename sans extension, lowercased)
  4) Detect join key across reports (prefers id/email/address/space/unit, coverage/uniqueness weighted)
  5) Build output rows:
       - If join key found: iterate entity keys, pull mapped column from target report row
       - Else: align by row position
       - Column resolution uses exact/normalized/variation/fuzzy matching
  6) Apply template ordering (or mapping order)
  7) Export to temp Excel, stream back, cleanup
```

---

## Mapping logic (strict)
- Columns required in `mapping.xlsx`:
  - `output_col`: target column name in the final output
  - `report_name`: exact file name you expect the value to come from (normalized: lowercase, no extension)
  - `column_name`: primary column name to match in that report
  - `possible_variations`: comma/semicolon-separated synonyms for fuzzy matching
  - `default_value` (mandatory column): used only when `report_name` or `column_name` is blank for that row. We do **not** fall back to defaults if a lookup fails; defaults are explicit-only.
- Report matching: we normalize the uploaded filename (lowercase, trimmed, drop extension) and match against `report_name` normalized the same way.
- Column matching order: exact raw → exact normalized → variation raw → variation normalized → partial/fuzzy contains on normalized names.
- Row matching: if a join key is found across reports, we use that key’s value to select the correct row from each report; otherwise rows are aligned by index.

---

## Architecture diagram (text)
```
Frontend (Next.js)
  - File inputs (mapping, template, data files)
  - POST /process FormData
        |
        v
Backend (FastAPI, main.py)
  - parse_mapping (strict schema)
  - parse_template (optional ordering)
  - read_input_file -> clean_dataframe (header detection, cleaning)
  - find_matching_column (exact/normalized/variation/fuzzy)
  - select_join_key + collect_master_keys (row resolution)
  - build_output_from_mapping (mapping-driven extraction)
  - coerce_column_types (pass-through)
  - Excel export + temp cleanup
```

## Validation & enrichment rules (current)
- Status: `Occupied` if either First Name or Last Name is present; otherwise `Vacant`.
- Space defaults: For occupied rows only, missing `Width`/`Length` default to `1`; `Space Size` is computed and highlighted red when a default was used.
- Access codes & phones:
  - Priority: keep existing Access Code; else use last 4 of `Cell Phone`; else last 4 of `Alt Cell Phone`; else generate a unique, valid 10-digit US phone and store it in `Cell Phone`, then use its last 4.
  - Applies only to occupied rows; vacant rows are not backfilled.
  - Derived/generated Access Codes and generated Cell Phones are highlighted blue. Access Codes are stored as integers when generated.
- Invalid data tracking:
  - Invalid cells are highlighted red.
  - An `Invalid Entries` worksheet is added with columns `column` and `invalid rows`, headers bolded, wrapped, and fully bordered; data rows are wrapped and fully bordered as well.
