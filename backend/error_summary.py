from __future__ import annotations

from typing import Dict, List

from openpyxl.styles import Border, Font, Side # type: ignore #typ


def _apply_table_style(ws, start_row: int, end_row: int, end_col: int) -> None:
    header_font = Font(bold=True)
    thin_side = Side(style="thin")
    full_border = Border(top=thin_side, bottom=thin_side, left=thin_side, right=thin_side)

    for row in ws.iter_rows(min_row=start_row, max_row=end_row, max_col=end_col):
        for cell in row:
            if cell.row == start_row:
                cell.font = header_font
            cell.border = full_border


def write_error_summary_sheet(wb, invalid_reasons: List[Dict[str, object]]) -> None:
    if not invalid_reasons:
        return

    if "Error Summary" in wb.sheetnames:
        del wb["Error Summary"]
    ws = wb.create_sheet("Error Summary")

    ws.append(["column", "Space ID", "row_number", "reason", "value"])
    invalid_reasons_sorted = sorted(
        invalid_reasons, key=lambda r: (str(r.get("column", "")), r.get("row_index", 0))
    )
    for item in invalid_reasons_sorted:
        row_number = item.get("row_index", 0) + 2
        ws.append(
            [
                item.get("column", ""),
                item.get("space", ""),
                row_number,
                item.get("reason", ""),
                item.get("value", ""),
            ]
        )

    _apply_table_style(ws, 1, ws.max_row, 6)
