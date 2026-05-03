from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from competitors import refresh_competitors


HISTORY_DIR = Path("data")
HISTORY_PATH = HISTORY_DIR / "history.csv"
CATEGORY_HISTORY_PATH = HISTORY_DIR / "category_history.csv"

HISTORY_COLUMNS = [
    "snapshot_date",
    "snapshot_datetime",
    "competitor_code",
    "competitor_name",
    "entity_role",
    "count",
    "total_area",
    "avg_price",
    "total_price",
    "unconfirmed_count",
    "removed_count",
    "data_freshness",
    "last_checked_at",
]

CATEGORY_HISTORY_COLUMNS = [
    "snapshot_date",
    "snapshot_datetime",
    "competitor_code",
    "competitor_name",
    "entity_role",
    "category_code",
    "category_name",
    "count",
    "total_area",
    "avg_price",
    "total_price",
    "data_freshness",
    "last_checked_at",
]

CATEGORY_LABELS = {
    "commercial": "Офисы / свободного назначения / торговые",
    "industrial": "Производства / склады",
}


def ensure_history_storage() -> Path:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    if not HISTORY_PATH.exists():
        pd.DataFrame(columns=HISTORY_COLUMNS).to_csv(HISTORY_PATH, index=False, encoding="utf-8-sig")
    if not CATEGORY_HISTORY_PATH.exists():
        pd.DataFrame(columns=CATEGORY_HISTORY_COLUMNS).to_csv(CATEGORY_HISTORY_PATH, index=False, encoding="utf-8-sig")
    return HISTORY_PATH


def ensure_category_history_storage() -> Path:
    ensure_history_storage()
    return CATEGORY_HISTORY_PATH


def _read_csv(path: Path, columns: List[str]) -> pd.DataFrame:
    if not path.exists():
        pd.DataFrame(columns=columns).to_csv(path, index=False, encoding="utf-8-sig")
    df = pd.read_csv(path, encoding="utf-8-sig")
    if df.empty:
        return pd.DataFrame(columns=columns)
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    return df


def read_history() -> pd.DataFrame:
    ensure_history_storage()
    df = _read_csv(HISTORY_PATH, HISTORY_COLUMNS)
    for column in ("count", "total_area", "avg_price", "total_price", "unconfirmed_count", "removed_count"):
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    for column in ("data_freshness", "last_checked_at", "competitor_code", "competitor_name", "snapshot_date", "snapshot_datetime", "entity_role"):
        df[column] = df[column].fillna("")
    return df


def read_category_history() -> pd.DataFrame:
    ensure_category_history_storage()
    df = _read_csv(CATEGORY_HISTORY_PATH, CATEGORY_HISTORY_COLUMNS)
    for column in ("count", "total_area", "avg_price", "total_price"):
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    for column in ("data_freshness", "last_checked_at", "competitor_code", "competitor_name", "snapshot_date", "snapshot_datetime", "entity_role", "category_code", "category_name"):
        df[column] = df[column].fillna("")
    return df


def _category_code(room_type: str) -> str:
    text = str(room_type or "").lower().replace("ё", "е")
    if any(word in text for word in ("офис", "универс", "свобод", "торгов", "ритейл", "retail", "office")):
        return "commercial"
    if any(word in text for word in ("склад", "производ", "warehouse", "production")):
        return "industrial"
    return "other"


def _category_label(code: str) -> str:
    return CATEGORY_LABELS.get(code, "Прочее / не указано")


def _category_records_from_stats(base_record: Dict, stats: Dict) -> List[Dict]:
    grouped: Dict[str, Dict[str, float]] = {
        code: {"count": 0, "total_area": 0.0, "total_price": 0.0} for code in CATEGORY_LABELS
    }

    by_type = stats.get("by_type") or {}
    for room_type, row in by_type.items():
        code = _category_code(str(room_type))
        grouped.setdefault(code, {"count": 0, "total_area": 0.0, "total_price": 0.0})
        grouped[code]["count"] += int(row.get("count", 0) or 0)
        grouped[code]["total_area"] += float(row.get("area", row.get("total_area", 0)) or 0)
        grouped[code]["total_price"] += float(row.get("total_price", 0) or 0)

    records: List[Dict] = []
    for code, values in grouped.items():
        total_area = round(float(values.get("total_area", 0) or 0), 2)
        total_price = round(float(values.get("total_price", 0) or 0), 2)
        records.append({
            "snapshot_date": base_record["snapshot_date"],
            "snapshot_datetime": base_record["snapshot_datetime"],
            "competitor_code": base_record["competitor_code"],
            "competitor_name": base_record["competitor_name"],
            "entity_role": base_record["entity_role"],
            "category_code": code,
            "category_name": _category_label(code),
            "count": int(values.get("count", 0) or 0),
            "total_area": total_area,
            "avg_price": round(total_price / total_area, 2) if total_area > 0 and total_price > 0 else 0.0,
            "total_price": total_price,
            "data_freshness": base_record.get("data_freshness", ""),
            "last_checked_at": base_record.get("last_checked_at", ""),
        })
    return records


def _upsert_category_rows(base_record: Dict, stats: Dict) -> None:
    new_rows = _category_records_from_stats(base_record, stats)
    df = read_category_history()
    if not df.empty:
        mask = (df["snapshot_date"] == base_record["snapshot_date"]) & (df["competitor_code"] == base_record["competitor_code"])
        df = df.loc[~mask].copy()
    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    df = df.sort_values(["category_code", "competitor_code", "snapshot_date", "snapshot_datetime"], ascending=[True, True, True, True])
    ensure_category_history_storage()
    df.to_csv(CATEGORY_HISTORY_PATH, index=False, encoding="utf-8-sig")


def upsert_weekly_snapshot(competitor_code: str, competitor_name: str, stats: Dict, lifecycle: Optional[Dict] = None, freshness: str = "", entity_role: str = "competitor") -> Dict:
    now = datetime.now()
    snapshot_date = now.strftime("%Y-%m-%d")
    snapshot_datetime = now.strftime("%Y-%m-%d %H:%M:%S")
    lifecycle = lifecycle or {}

    record = {
        "snapshot_date": snapshot_date,
        "snapshot_datetime": snapshot_datetime,
        "competitor_code": competitor_code,
        "competitor_name": competitor_name,
        "entity_role": entity_role,
        "count": int(stats.get("count", 0) or 0),
        "total_area": float(stats.get("total_area", 0) or 0),
        "avg_price": float(stats.get("avg_price", 0) or 0),
        "total_price": float(stats.get("total_price", 0) or 0),
        "unconfirmed_count": int(lifecycle.get("unconfirmed_count", 0) or 0),
        "removed_count": int(lifecycle.get("removed_count", 0) or 0),
        "data_freshness": str(freshness or ""),
        "last_checked_at": str(lifecycle.get("manual_checked_at") or snapshot_datetime),
    }

    df = read_history()
    if not df.empty:
        mask = (df["snapshot_date"] == snapshot_date) & (df["competitor_code"] == competitor_code)
        df = df.loc[~mask].copy()

    df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
    df = df.sort_values(["competitor_code", "snapshot_date", "snapshot_datetime"], ascending=[True, True, True])
    ensure_history_storage()
    df.to_csv(HISTORY_PATH, index=False, encoding="utf-8-sig")
    _upsert_category_rows(record, stats)
    return record


def get_competitor_history(competitor_code: str) -> List[Dict]:
    df = read_history()
    if df.empty:
        return []
    df = df.loc[df["competitor_code"] == competitor_code].copy()
    if df.empty:
        return []
    df = df.sort_values(["snapshot_date", "snapshot_datetime"])
    return df.to_dict("records")


def get_competitor_category_history(competitor_code: str) -> List[Dict]:
    df = read_category_history()
    if df.empty:
        return []
    df = df.loc[df["competitor_code"] == competitor_code].copy()
    if df.empty:
        return []
    df = df.sort_values(["category_code", "snapshot_date", "snapshot_datetime"])
    return df.to_dict("records")


def get_category_comparison_history(category_code: str = "") -> List[Dict]:
    df = read_category_history()
    if df.empty:
        return []
    if category_code:
        df = df.loc[df["category_code"] == category_code].copy()
    competitors = refresh_competitors()
    df["entity_role"] = df.apply(lambda row: str(row.get("entity_role") or (competitors.get(str(row.get("competitor_code") or "")) or {}).get("entity_role") or "competitor"), axis=1)
    df["competitor_name"] = df.apply(lambda row: str(row.get("competitor_name") or (competitors.get(str(row.get("competitor_code") or "")) or {}).get("name") or row.get("competitor_code") or ""), axis=1)
    df = df.sort_values(["category_code", "snapshot_date", "competitor_name", "snapshot_datetime"])
    return df.to_dict("records")


def get_portfolio_history() -> List[Dict]:
    df = read_history()
    if df.empty:
        return []

    def join_freshness(values):
        values = [str(x) for x in values if str(x)]
        return ", ".join(sorted(set(values)))

    grouped = (
        df.groupby("snapshot_date", as_index=False)
        .agg({"count": "sum", "total_area": "sum", "total_price": "sum", "competitor_code": "nunique", "unconfirmed_count": "sum", "removed_count": "sum", "data_freshness": join_freshness})
        .rename(columns={"competitor_code": "competitors_included"})
    )
    grouped["avg_price"] = 0.0
    mask = grouped["total_area"] > 0
    grouped.loc[mask, "avg_price"] = grouped.loc[mask, "total_price"] / grouped.loc[mask, "total_area"]
    grouped["competitor_name"] = "Вся база"
    grouped = grouped[["snapshot_date", "count", "total_area", "avg_price", "total_price", "competitors_included", "unconfirmed_count", "removed_count", "data_freshness", "competitor_name"]]
    grouped = grouped.sort_values("snapshot_date")
    return grouped.to_dict("records")


def get_role_comparison_history() -> List[Dict]:
    df = read_history()
    if df.empty:
        return []
    competitors = refresh_competitors()
    if "entity_role" not in df.columns:
        df["entity_role"] = ""
    df["entity_role"] = df.apply(lambda row: str(row.get("entity_role") or (competitors.get(str(row.get("competitor_code") or "")) or {}).get("entity_role") or "competitor"), axis=1)
    df["competitor_name"] = df.apply(lambda row: str(row.get("competitor_name") or (competitors.get(str(row.get("competitor_code") or "")) or {}).get("name") or row.get("competitor_code") or ""), axis=1)
    for column in ("count", "total_area", "avg_price", "total_price", "unconfirmed_count", "removed_count"):
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    df = df.sort_values(["snapshot_date", "competitor_name", "snapshot_datetime"])
    return df.to_dict("records")
