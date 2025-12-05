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
Use this script to create sample mapping/template/data files for quick testing.

`scripts/generate_dummy_files.py`
```python
import pandas as pd
from pathlib import Path

def main(out_dir="samples"):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    mapping = pd.DataFrame([
        {"source_col": "id,id_number", "output_col": "id", "default": ""},
        {"source_col": "full_name,name", "output_col": "name", "default": "N/A"},
        {"source_col": "age", "output_col": "age", "default": ""},
        {"source_col": "city", "output_col": "city", "default": "Unknown"},
    ])
    mapping.to_excel(out / "mapping.xlsx", index=False)

    template = pd.DataFrame({"output_col": ["id", "name", "age", "city"]})
    template.to_excel(out / "template.xlsx", index=False)

    file1 = pd.DataFrame({
        "id_number": [1, 2],
        "full_name": ["Alice", "Bob"],
        "age": [23, None],
    })
    file1.to_excel(out / "data1.xlsx", index=False)

    file2 = pd.DataFrame({
        "id": [3],
        "name": ["Carol"],
        "city": ["LA"],
    })
    file2.to_csv(out / "data2.csv", index=False)

    print(f"Dummy files written to {out.resolve()}")

if __name__ == "__main__":
    main()
```

Run:
```bash
python scripts/generate_dummy_files.py
```
Then upload the generated files from the `samples/` directory via the UI or curl example.

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
