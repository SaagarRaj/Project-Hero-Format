# Excel Mapping Normalizer

FastAPI + Next.js app that merges multiple CSV/Excel reports into a single normalized Excel output using a strict mapping file. It also validates and enriches specific columns (phones, money, dates, state, ZIP, access code, space size) and marks issues directly in the output workbook.

## What it does
- Accepts a required `mapping.xlsx`, optional `template.xlsx`, and one or more data files (CSV/XLS/XLSX).
- Cleans messy headers by auto-detecting the header row.
- Resolves columns using exact/normalized/variation/fuzzy matching.
- Joins reports by a detected key when possible, otherwise aligns rows by position.
- Applies validation, normalization, and derived-field logic.
- Returns `final_output.xlsx` with cell highlights and an optional `Invalid Entries` sheet.

## Quick start (Docker)
```bash
docker-compose up --build
```
- Frontend: `http://localhost:3000`
- Backend docs: `http://localhost:8000/docs`

## Local development
Backend:
```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Frontend:
```bash
cd frontend
npm install
npm run dev
```

Set `NEXT_PUBLIC_BACKEND_URL` if the backend is not on `http://localhost:8000`.

## API usage
```bash
curl -X POST http://localhost:8000/process \
  -F "mapping=@mapping.xlsx" \
  -F "template=@template.xlsx" \
  -F "files=@data1.xlsx" \
  -F "files=@data2.csv" \
  --output final_output.xlsx
```

## Mapping file (strict format)
`mapping.xlsx` must include these columns:
- `output_col`
- `report_name`
- `column_name`
- `possible_variations`
- `default_value`

Rules:
- `report_name` must match the uploaded filename (normalized to lowercase, extension removed, extra spaces collapsed).
- `column_name` is the primary column name to pull from that report.
- `possible_variations` is a comma/semicolon-separated list of synonyms.
- `default_value` is only used when `report_name` or `column_name` is blank for that row.
- Rows missing `output_col` are ignored.
- If `report_name` or `column_name` is blank and `default_value` is blank, the row is ignored.
- Defaults are not used as a fallback if a lookup fails; they are only used for explicit default-only rows.

Example:
```
output_col | report_name | column_name | possible_variations | default_value
Unit ID    | rent roll   | unit id     | unit number,space   |
Status     |             |             |                     | Vacant
```

## Template file (optional)
`template.xlsx` contains a single `output_col` column that defines the output order. Missing template columns are created as empty. Any extra mapped columns not in the template are appended to the end.

## Header detection (messy files)
Input files are read without headers. The app scans up to 25 rows and scores each row based on:
- non-null density
- string density and uniqueness
- presence of mapping tokens/variations
- column-like words (name, date, id, unit, etc.)

The highest scoring row becomes the header; rows above it are discarded.

## Join strategy
The backend looks for a shared key across reports using normalized column names present in at least two reports. It scores candidates by:
- coverage and uniqueness
- name hints (id, email, address, space, unit)

If a join key is found, rows are merged by that key. Otherwise, rows are aligned by index.

## Validation and enrichment
After merging, the output is normalized using `app/backend/validation.py`.

### Column cleaning
Only columns with these exact names are cleaned:
- Phone: `Cell Phone`, `Home Phone`, `Work Phone`, `Alt Home Phone`, `Alt Work Phone`, `Alt Cell Phone`, `Lien Holder Phone`, `Commanding Officer Phone`, `Military Unit Phone`
- Currency: `Rate`, `Web Rate`, `Rent`, `Security Deposit`, `Security Deposit Balance`, `Rent Balance`, `Fees Balance`, `Protection/Insurance Balance`, `Merchandise Balance`, `Late Fees Balance`, `Lien Fees Balance`, `Tax Balance`, `Prepaid Rent`, `Prepaid Additional Rent/Premium`, `Prepaid Tax`, `Additional Rent/Premium`, `Discount Value`, `Promotion Value`, `AutoPayAmt`, `Protection/Insurance Coverage`
- Numbers: `Width`, `Length`, `Height`, `Door Width`, `Door Height`, `Promotion Length`, `Account Code`, `Access Code`, `Sq. Ft.`
- Dates (MM/DD/YY): `DOB`, `DL Exp Date`, `Last Rent Change Date`, `Move In Date`, `Move Out Date`, `Paid Date`, `Paid Through Date`, `Lien Posted Date`, `Promotion Start`, `start_date`, `pay_by_date`, `end_date`, `UnitStartDate`
- Emails: `Email`, `Alt Email`, `Lien Holder Email`, `Commanding Officer Email`, `Military Email`
- States: `State`, `DL State`, `Alt State`, `Lien Holder State`, `Military Unit State` (normalized to 2-letter code)
- ZIP: `ZIP`, `Alt ZIP` (must be 5 digits)

Invalid values are preserved but marked as invalid (see highlights).

### Derived fields and rules
- `Space Category` (format `5X5-SelfStorage`) populates `Width`, `Length`, `Space Type`, then clears `Space Category`.
- `Status` becomes `Occupied` if `First Name` or `Last Name` is present, else `Vacant`.
- `Access Code` for occupied rows:
  - Keep existing value if present.
  - Otherwise use last 4 of `Cell Phone`.
  - Else last 4 of `Alt Cell Phone`.
  - Else generate a unique 10-digit US phone, store in `Cell Phone`, and use its last 4.
- `Width`/`Length` defaults to `1` for occupied rows if missing.
- `Space Size` becomes `[Width x Length]`. Rows using default width/length are flagged.
- `Sq. Ft.` = `Width * Length` when both are present.
- `Bill Day` = day-of-month after `Paid Through Date`.
- If `State` is a valid US abbreviation, `Country` is set to `USA`.

## Output highlights
- Red fill: invalid values and `Space Size`/`Sq. Ft.` derived from defaults.
- Blue fill: derived `Access Code` and generated `Cell Phone`.
- `Invalid Entries` sheet is added when invalid data exists (column name + Excel row numbers).

## Project structure
```
/backend
  main.py
  validation.py
  requirements.txt
  Dockerfile
/frontend
  app/page.jsx
  app/globals.css
  components/ui/...
  package.json
  Dockerfile
docker-compose.yml
```
