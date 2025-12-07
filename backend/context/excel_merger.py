"""
Advanced Excel/CSV Data Merger with mapping-driven extraction.

Key features kept from the previous version:
- Header-row detection and metadata skipping.
- Column cleaning/normalization helpers.
- Per-report loading with light cleansing (phone/email/unit normalization).
- Merge-like row resolution using detected join keys.

New/updated behaviors:
- Strict mapping format (output_col, report_name, column_name, possible_variations).
- Dynamic column resolution (no hard-coding).
- Row-level matching across reports using detected join keys.
- Mapping-driven output construction (one column per mapping row).
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def normalize_column_name(name: str) -> str:
    """Lowercase, trim, collapse spaces, and strip non-alphanum (keeps spaces)."""
    cleaned = re.sub(r"\s+", " ", str(name or "").strip().lower())
    cleaned = re.sub(r"[^a-z0-9 #/_-]", "", cleaned)
    return cleaned


def find_matching_column(
    df: pd.DataFrame, base_name: str, variations: Iterable[str]
) -> Optional[str]:
    """
    Resolve the best matching column in df for the given base_name/variations.
    - Exact match on raw column names.
    - Exact match on normalized names.
    - Variation matches (raw then normalized).
    - Partial/fuzzy: contains match on normalized names.
    Returns the original column name if found, else None.
    """
    if df is None or df.empty:
        return None

    base_name = base_name or ""
    variation_list = [v for v in variations if v] if variations else []

    # Precompute normalized lookup.
    normalized_lookup = {normalize_column_name(col): col for col in df.columns}

    # 1) Exact raw match
    if base_name in df.columns:
        return base_name

    # 2) Exact normalized match
    base_norm = normalize_column_name(base_name)
    if base_norm in normalized_lookup:
        return normalized_lookup[base_norm]

    # 3) Variations exact
    for v in variation_list:
        if v in df.columns:
            return v
        v_norm = normalize_column_name(v)
        if v_norm in normalized_lookup:
            return normalized_lookup[v_norm]

    # 4) Partial/fuzzy on normalized names
    candidates = []
    for norm, original in normalized_lookup.items():
        score = 0
        if base_norm and base_norm in norm:
            score += 2
        for v in variation_list:
            v_norm = normalize_column_name(v)
            if v_norm and v_norm in norm:
                score += 1
        if score > 0:
            candidates.append((score, original))

    if candidates:
        candidates.sort(key=lambda x: (-x[0], x[1]))
        return candidates[0][1]

    return None


class AdvancedExcelDataMerger:
    """Advanced Excel data merger with mapping-driven extraction."""

    def __init__(self):
        self.mapping_rules: List[Dict[str, str]] = []
        self.mapping_df: Optional[pd.DataFrame] = None
        self.source_dataframes: Dict[str, pd.DataFrame] = {}
        self.normalized_columns: Dict[str, Dict[str, str]] = {}
        self.output_columns: List[str] = []
        self.merged_df: Optional[pd.DataFrame] = None

    # -------------------------------------------------------------------------
    # Data standardization helpers (unchanged behavior)
    # -------------------------------------------------------------------------
    def standardize_date_format(self, value, output_format: str = "%Y-%m-%d"):
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

    def standardize_phone(self, phone):
        if pd.isna(phone) or phone == "" or str(phone).strip() == "":
            return ""
        digits = "".join(filter(str.isdigit, str(phone)))
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        if len(digits) == 11 and digits[0] == "1":
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        return str(phone)

    def clean_currency(self, value):
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

    def standardize_unit_number(self, unit):
        if pd.isna(unit) or unit == "":
            return ""
        unit_str = str(unit).strip().upper()
        unit_str = re.sub(r"\s+", " ", unit_str)
        return unit_str

    def validate_email(self, email):
        if pd.isna(email) or email == "" or str(email).strip() == "":
            return ""
        email_str = str(email).strip().lower()
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return email_str if re.match(pattern, email_str) else ""

    # -------------------------------------------------------------------------
    # Mapping/template loading
    # -------------------------------------------------------------------------
    def load_mapping_from_file(self, mapping_file) -> List[Dict[str, str]]:
        """
        Load mapping using the strict format:
        output_col, report_name, column_name, possible_variations
        """
        try:
            temp_path = self._save_temp_file(mapping_file)
            mapping_df = pd.read_excel(temp_path, sheet_name=0)
            os.unlink(temp_path)
        except Exception as exc:
            logger.error(f"Error loading mapping file: {exc}")
            raise

        required = {"output_col", "report_name", "column_name", "possible_variations"}
        normalized_cols = {normalize_column_name(c): c for c in mapping_df.columns}
        if not required.issubset(set(normalized_cols.keys())):
            missing = required - set(normalized_cols.keys())
            raise ValueError(f"Mapping file missing required columns: {', '.join(sorted(missing))}")

        def get_col(name: str) -> str:
            return mapping_df[normalized_cols[name]]

        rules: List[Dict[str, str]] = []
        for _, row in mapping_df.iterrows():
            output_col = str(row.get(normalized_cols["output_col"], "")).strip()
            report_name = str(row.get(normalized_cols["report_name"], "")).strip()
            column_name = str(row.get(normalized_cols["column_name"], "")).strip()
            variations_raw = str(row.get(normalized_cols["possible_variations"], "")).strip()
            if not output_col or not report_name or not column_name:
                continue
            variations = [v.strip() for v in variations_raw.replace(";", ",").split(",") if v.strip()] if variations_raw else []
            rules.append(
                {
                    "output_col": output_col,
                    "report_name": report_name,
                    "column_name": column_name,
                    "variations": variations,
                }
            )

        if not rules:
            raise ValueError("Mapping file contained no usable rows.")

        self.mapping_df = mapping_df
        self.mapping_rules = rules
        logger.info(f"Loaded {len(rules)} mapping rows for {len(set(r['report_name'] for r in rules))} reports")
        return rules

    def load_output_template_from_file(self, template_file) -> List[str]:
        try:
            temp_path = self._save_temp_file(template_file)
            template_df = pd.read_excel(temp_path, sheet_name=0, nrows=0)
            os.unlink(temp_path)
            columns = template_df.columns.tolist()
            self.output_columns = columns
            logger.info(f"Loaded {len(columns)} output columns from template")
            return columns
        except Exception as exc:
            logger.error(f"Error loading template file: {exc}")
            raise

    # -------------------------------------------------------------------------
    # Header detection and loading
    # -------------------------------------------------------------------------
    def find_header_row(self, file_path: str, sheet_name: str = 0) -> int:
        try:
            df_preview = pd.read_excel(
                file_path, sheet_name=sheet_name, header=None, nrows=20, engine="openpyxl"
            )
            best_row = 0
            best_score = 0
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
            ]
            for idx in range(len(df_preview)):
                row = df_preview.iloc[idx]
                if row.isna().all():
                    continue
                non_null_count = row.notna().sum()
                string_count = sum(isinstance(val, str) and len(str(val).strip()) > 0 for val in row)
                unique_count = len(row.dropna().unique())
                column_like_count = 0
                for val in row:
                    if isinstance(val, str):
                        val_lower = val.lower().strip()
                        if len(val_lower) < 50 and any(word in val_lower for word in common_column_words):
                            column_like_count += 1
                        elif len(val_lower.split()) <= 4 and len(val_lower) < 50:
                            column_like_count += 0.5
                if non_null_count < 3:
                    continue
                numeric_count = sum(isinstance(val, (int, float)) and not pd.isna(val) for val in row)
                score = (string_count * 3) + (unique_count * 2) + non_null_count
                score += column_like_count * 5
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
            logger.info(f"Auto-detected header row at index {best_row} (score: {best_score})")
            return best_row
        except Exception as exc:
            logger.warning(f"Error auto-detecting header row: {exc}. Defaulting to row 0")
            return 0

    def load_source_file(
        self,
        file,
        report_name: str,
        sheet_name: str = 0,
        header_row: int = None,
    ) -> pd.DataFrame:
        try:
            temp_path = self._save_temp_file(file)
            if header_row is None:
                header_row = self.find_header_row(temp_path, sheet_name)
                logger.info(f"Auto-detected header row {header_row} for {report_name}")
            df = pd.read_excel(temp_path, sheet_name=sheet_name, header=header_row, engine="openpyxl")
            os.unlink(temp_path)

            df.columns = [str(col).strip() for col in df.columns]
            df = df.dropna(how="all")
            df = self.cleanse_data(df, report_name)

            self.source_dataframes[report_name] = df
            self.normalized_columns[report_name] = {normalize_column_name(c): c for c in df.columns}
            logger.info(f"Loaded {report_name}: {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as exc:
            logger.error(f"Error loading source file {report_name}: {exc}")
            raise

    def cleanse_data(self, df: pd.DataFrame, report_name: str) -> pd.DataFrame:
        df_clean = df.copy()
        cleansing_actions = []

        phone_cols = [col for col in df_clean.columns if "phone" in col.lower()]
        for col in phone_cols:
            df_clean[col] = df_clean[col].apply(self.standardize_phone)
            cleansing_actions.append(f"Standardized {col}")

        email_cols = [col for col in df_clean.columns if "email" in col.lower() or "e-mail" in col.lower()]
        for col in email_cols:
            df_clean[col] = df_clean[col].apply(self.validate_email)
            cleansing_actions.append(f"Validated {col}")

        unit_cols = [
            col
            for col in df_clean.columns
            if any(k in col.lower() for k in ["unit", "space", "unit #", "unit number", "space number"])
        ]
        for col in unit_cols:
            df_clean[col] = df_clean[col].apply(self.standardize_unit_number)
            cleansing_actions.append(f"Standardized {col}")

        key_columns = []
        for potential_key in ["Tenant ID", "Space", "Unit", "Unit #", "Unit Number", "Tenant Name"]:
            if potential_key in df_clean.columns:
                key_columns.append(potential_key)
        if key_columns:
            before = len(df_clean)
            df_clean = df_clean.drop_duplicates(subset=key_columns, keep="last")
            after = len(df_clean)
            if before > after:
                cleansing_actions.append(f"Removed {before - after} duplicate records")

        if cleansing_actions:
            logger.info(f"Data cleansing for {report_name}: {', '.join(cleansing_actions)}")
        return df_clean

    # -------------------------------------------------------------------------
    # Type inference (kept)
    # -------------------------------------------------------------------------
    def infer_and_apply_data_types(self, df: pd.DataFrame, output_columns: List[str] = None) -> pd.DataFrame:
        df_processed = df.copy()
        type_summary = {"numeric": 0, "datetime": 0, "boolean": 0, "string": 0, "id": 0}
        for col in df_processed.columns:
            if df_processed[col].isna().all():
                continue
            non_null_values = df_processed[col].dropna()
            if len(non_null_values) == 0:
                continue
            col_lower = col.lower()
            if any(
                keyword in col_lower
                for keyword in [
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
            ):
                try:
                    df_processed[col] = df_processed[col].apply(lambda x: self.clean_currency(x))
                    type_summary["numeric"] += 1
                except Exception:
                    pass
            elif any(
                keyword in col_lower
                for keyword in [
                    "date",
                    "time",
                    "day",
                    "dt",
                    "on",
                    "through",
                    "thru",
                    "paid",
                    "expiration",
                    "effective",
                    "start",
                    "end",
                    "move",
                ]
            ):
                try:
                    df_processed[col] = df_processed[col].apply(
                        lambda x: self.standardize_date_format(x) if pd.notna(x) and x != "" else ""
                    )
                    type_summary["datetime"] += 1
                except Exception:
                    pass
            elif any(keyword in col_lower for keyword in ["id", "number", "code", "#", "serial", "policy", "lease"]):
                df_processed[col] = df_processed[col].astype(str).replace("nan", "")
                df_processed[col] = df_processed[col].fillna("")
                type_summary["id"] += 1
            elif any(
                keyword in col_lower
                for keyword in ["flag", "enabled", "alarm", "access", "paperless", "autopay", "offline", "business"]
            ):
                try:
                    df_processed[col] = df_processed[col].map(
                        {
                            "Yes": True,
                            "No": False,
                            "Y": True,
                            "N": False,
                            "yes": True,
                            "no": False,
                            "true": True,
                            "false": False,
                            "TRUE": True,
                            "FALSE": False,
                            "1": True,
                            "0": False,
                            1: True,
                            0: False,
                        }
                    )
                    df_processed[col] = df_processed[col].fillna(False)
                    type_summary["boolean"] += 1
                except Exception:
                    pass
            else:
                if df_processed[col].dtype == "object":
                    df_processed[col] = df_processed[col].astype(str).replace("nan", "")
                    df_processed[col] = df_processed[col].fillna("")
                    type_summary["string"] += 1
        logger.info(
            f"  Data types applied: {type_summary['numeric']} numeric, {type_summary['datetime']} datetime, "
            f"{type_summary['boolean']} boolean, {type_summary['id']} ID, {type_summary['string']} string"
        )
        return df_processed

    # -------------------------------------------------------------------------
    # Join key detection and row resolution
    # -------------------------------------------------------------------------
    def _select_join_key(self) -> Optional[Tuple[str, Dict[str, str]]]:
        """
        Detect a join key across source dataframes.
        Returns (normalized_key, per_report_original_name) or (None, {}).
        """
        if not self.source_dataframes:
            return None, {}

        # Collect candidate normalized names that appear in >=2 reports.
        presence: Dict[str, List[str]] = {}
        for report, norm_map in self.normalized_columns.items():
            for norm in norm_map.keys():
                presence.setdefault(norm, []).append(report)

        candidates = {norm: reps for norm, reps in presence.items() if len(reps) >= 2}
        if not candidates:
            return None, {}

        def score_norm(norm: str, reports: List[str]) -> float:
            # Higher score for more reports, higher uniqueness, lower nulls, and name bonus.
            name_bonus = 0
            if any(k in norm for k in ["id", "email", "address", "space", "unit"]):
                name_bonus += 1
            uniques = []
            coverages = []
            for rep in reports:
                df = self.source_dataframes[rep]
                col = self.normalized_columns[rep][norm]
                series = df[col]
                non_null = series.dropna()
                if len(series) == 0:
                    continue
                uniques.append(non_null.nunique() / max(len(non_null), 1))
                coverages.append(len(non_null) / max(len(series), 1))
            avg_unique = np.mean(uniques) if uniques else 0
            avg_cov = np.mean(coverages) if coverages else 0
            return (len(reports) * 2) + avg_unique + avg_cov + name_bonus

        best_norm = None
        best_score = -1
        for norm, reps in candidates.items():
            sc = score_norm(norm, reps)
            if sc > best_score:
                best_score = sc
                best_norm = norm

        if not best_norm:
            return None, {}

        per_report = {
            rep: self.normalized_columns[rep][best_norm]
            for rep in presence.get(best_norm, [])
            if best_norm in self.normalized_columns[rep]
        }
        logger.info(f"Selected join key '{best_norm}' mapped per report: {per_report}")
        return best_norm, per_report

    def _collect_master_keys(self, join_key_norm: Optional[str], per_report_key: Dict[str, str]) -> List:
        if not join_key_norm:
            return []
        keys = set()
        for report, df in self.source_dataframes.items():
            col = per_report_key.get(report)
            if not col or col not in df.columns:
                continue
            valid_keys = df[col].dropna()
            valid_keys = [v for v in valid_keys if str(v).strip() != ""]
            keys.update(valid_keys)
        master_keys = list(keys)
        logger.info(f"Collected {len(master_keys)} master keys using join column '{join_key_norm}'")
        return master_keys

    # -------------------------------------------------------------------------
    # Output construction
    # -------------------------------------------------------------------------
    def build_output_from_mapping(self) -> pd.DataFrame:
        """
        Build the final output DataFrame using mapping rules and row-level matching.
        """
        if not self.mapping_rules:
            raise ValueError("Mapping rules not loaded.")
        if not self.source_dataframes:
            raise ValueError("No source dataframes loaded.")

        join_key_norm, per_report_key = self._select_join_key()
        master_keys = self._collect_master_keys(join_key_norm, per_report_key)

        # Prepare column resolution cache per report.
        resolved_columns: Dict[str, Dict[Tuple[str, str], Optional[str]]] = {}

        def resolve_column(report: str, base: str, variations: List[str]) -> Optional[str]:
            cache = resolved_columns.setdefault(report, {})
            key = (base, ",".join(variations))
            if key in cache:
                return cache[key]
            df = self.source_dataframes.get(report)
            col = find_matching_column(df, base, variations) if df is not None else None
            cache[key] = col
            return col

        output_rows = []

        if master_keys:
            # Row-level matching using detected keys.
            for entity_key in master_keys:
                row_out: Dict[str, object] = {}
                for rule in self.mapping_rules:
                    out_col = rule["output_col"]
                    report = rule["report_name"]
                    base_col = rule["column_name"]
                    variations = rule.get("variations", [])
                    df = self.source_dataframes.get(report)
                    value = ""
                    if df is not None:
                        join_col = per_report_key.get(report)
                        target_col = resolve_column(report, base_col, variations)
                        if join_col and target_col and join_col in df.columns and target_col in df.columns:
                            matches = df[df[join_col] == entity_key]
                            if not matches.empty:
                                value = matches[target_col].iloc[0]
                        elif target_col and target_col in df.columns and not df.empty:
                            value = df[target_col].iloc[0]
                    row_out[out_col] = value
                output_rows.append(row_out)
        else:
            # Fallback: no join key; align rows by position based on the largest frame.
            max_len = max(len(df) for df in self.source_dataframes.values())
            for idx in range(max_len):
                row_out = {}
                for rule in self.mapping_rules:
                    out_col = rule["output_col"]
                    report = rule["report_name"]
                    base_col = rule["column_name"]
                    variations = rule.get("variations", [])
                    df = self.source_dataframes.get(report)
                    value = ""
                    if df is not None and len(df) > idx:
                        target_col = resolve_column(report, base_col, variations)
                        if target_col and target_col in df.columns:
                            value = df[target_col].iloc[idx]
                    row_out[out_col] = value
                output_rows.append(row_out)

        output_df = pd.DataFrame(output_rows)

        # Apply template order if available.
        if self.output_columns:
            for col in self.output_columns:
                if col not in output_df.columns:
                    output_df[col] = ""
            extras = [c for c in output_df.columns if c not in self.output_columns]
            output_df = output_df[self.output_columns + extras]

        output_df = self.infer_and_apply_data_types(output_df, self.output_columns)
        self.merged_df = output_df
        return output_df

    # -------------------------------------------------------------------------
    # Export and summaries
    # -------------------------------------------------------------------------
    def save_to_temp_file(self, df: pd.DataFrame) -> str:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        temp_path = temp_file.name
        temp_file.close()
        df_export = df.copy()
        for col in df_export.columns:
            if pd.api.types.is_datetime64_any_dtype(df_export[col]):
                df_export[col] = df_export[col].apply(lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else "")
        with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
            df_export.to_excel(writer, index=False, sheet_name="Merged Data")
            worksheet = writer.sheets["Merged Data"]
            for idx, col in enumerate(df_export.columns, 1):
                max_length = max(df_export[col].astype(str).map(len).max(), len(str(col)))
                worksheet.column_dimensions[worksheet.cell(1, idx).column_letter].width = min(max_length + 2, 50)
        logger.info(f"Saved output to temporary file: {temp_path}")
        return temp_path

    def _save_temp_file(self, uploaded_file) -> str:
        suffix = Path(uploaded_file.name).suffix
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        for chunk in uploaded_file.chunks():
            temp_file.write(chunk)
        temp_path = temp_file.name
        temp_file.close()
        return temp_path

    def get_merge_summary(self) -> Dict:
        if self.merged_df is None:
            return {}
        populated_cols = [col for col in self.merged_df.columns if self.merged_df[col].notna().any()]
        return {
            "total_rows": len(self.merged_df),
            "total_columns": len(self.merged_df.columns),
            "populated_columns": len(populated_cols),
            "data_coverage": round(
                len(populated_cols) / len(self.merged_df.columns) * 100, 2
            )
            if len(self.merged_df.columns) > 0
            else 0,
            "column_list": populated_cols[:20],
        }
