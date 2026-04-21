from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


RUB_FORMAT = '#,##0 "₽"'
RUB_M2_FORMAT = '#,##0.00 "₽/м²"'
AREA_FORMAT = '#,##0.0 "м²"'


def _apply_header_style(ws):
    fill = PatternFill(fill_type="solid", fgColor="D9EAD3")
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.fill = fill


def _autowidth(ws):
    for column_cells in ws.columns:
        max_len = 0
        col_idx = column_cells[0].column
        col_letter = get_column_letter(col_idx)
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            if len(value) > max_len:
                max_len = len(value)
        ws.column_dimensions[col_letter].width = min(max(max_len + 2, 14), 44)


def create_report(items: Iterable[dict], competitor: Optional[dict] = None, lifecycle: Optional[dict] = None, output_path: Optional[str] = None) -> str:
    items = list(items)
    competitor = competitor or {}
    lifecycle = lifecycle or {}

    if output_path is None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        safe_name = "".join(ch for ch in str(competitor.get("short_name") or competitor.get("name") or "report") if ch.isalnum() or ch in ("_", "-", " ")).strip() or "report"
        output_path = str(Path("reports") / f"{safe_name}_{timestamp}.xlsx")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for item in items:
        rows.append(
            {
                "Тип": item.get("type", ""),
                "Название": item.get("title", ""),
                "Площадь, м²": item.get("area"),
                "Ставка за м², ₽": item.get("price_value") or item.get("price_per_sqm"),
                "Общая стоимость, ₽": item.get("total_price_value") or item.get("total_price"),
                "Источник": item.get("source_label") or item.get("source_kind") or "",
                "Достоверность": item.get("reliability_label") or "",
                "Ссылка": item.get("source_url") or item.get("url") or "",
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["Тип", "Название", "Площадь, м²", "Ставка за м², ₽", "Общая стоимость, ₽", "Источник", "Достоверность", "Ссылка"])

    summary_rows = [
        {"Показатель": "Конкурент", "Значение": competitor.get("name", "")},
        {"Показатель": "Свободных помещений", "Значение": len(items)},
        {"Показатель": "Суммарная площадь", "Значение": round(sum(float(item.get("area", 0) or 0) for item in items), 2)},
        {
            "Показатель": "Средневзвешенная цена",
            "Значение": round(
                sum(float(item.get("total_price_value") or item.get("total_price") or 0) for item in items)
                / sum(float(item.get("area", 0) or 0) for item in items),
                2,
            )
            if sum(float(item.get("area", 0) or 0) for item in items) > 0
            else 0,
        },
        {"Показатель": "Неподтвержденных объектов", "Значение": int(lifecycle.get("unconfirmed_count", 0) or 0)},
        {"Показатель": "Объектов в архиве", "Значение": int(lifecycle.get("removed_count", 0) or 0)},
    ]
    df_summary = pd.DataFrame(summary_rows)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_summary.to_excel(writer, index=False, sheet_name="Сводка")
        df.to_excel(writer, index=False, sheet_name="Помещения")

        wb = writer.book
        for name in ("Сводка", "Помещения"):
            ws = wb[name]
            _apply_header_style(ws)
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            _autowidth(ws)

        ws_summary = wb["Сводка"]
        for row in range(2, ws_summary.max_row + 1):
            label = str(ws_summary[f"A{row}"].value or "").lower()
            cell = ws_summary[f"B{row}"]
            if "площад" in label:
                cell.number_format = AREA_FORMAT
            elif "цена" in label:
                cell.number_format = RUB_M2_FORMAT

        ws = wb["Помещения"]
        header_map = {str(ws.cell(row=1, column=col).value): col for col in range(1, ws.max_column + 1)}
        for row in range(2, ws.max_row + 1):
            for header, fmt in {
                "Площадь, м²": AREA_FORMAT,
                "Ставка за м², ₽": RUB_M2_FORMAT,
                "Общая стоимость, ₽": RUB_FORMAT,
            }.items():
                col = header_map.get(header)
                if col:
                    ws.cell(row=row, column=col).number_format = fmt
            link_col = header_map.get("Ссылка")
            if link_col:
                cell = ws.cell(row=row, column=link_col)
                if cell.value:
                    cell.hyperlink = str(cell.value)
                    cell.style = "Hyperlink"

    return output_path
