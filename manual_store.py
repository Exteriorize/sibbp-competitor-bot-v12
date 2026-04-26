from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4


DATA_DIR = Path("data")
MANUAL_COMPETITORS_PATH = DATA_DIR / "manual_competitors.json"
MANUAL_RECORDS_PATH = DATA_DIR / "manual_records.json"
MANUAL_ITEMS_PATH = DATA_DIR / "manual_items.json"

MANUAL_REVIEW_DAYS = 14
MANUAL_EXPIRE_DAYS = 28

SOURCE_OPTIONS = {
    "site": "Сайт",
    "avito": "Avito",
    "cian": "ЦИАН",
    "yandex_realty": "Яндекс Недвижимость",
    "2gis": "2ГИС",
    "yandex_maps": "Яндекс Карты",
    "call": "Звонок",
    "sign": "Фото вывески",
    "other": "Другое",
}

STATUS_OPTIONS = {
    "free": "Есть свободные помещения",
    "no_free": "Нет свободных помещений",
    "no_data": "Нет данных",
}

RELIABILITY_OPTIONS = {
    "high": "Высокая",
    "medium": "Средняя",
    "low": "Низкая",
}

ROOM_TYPE_OPTIONS = {
    "office": "Офис",
    "warehouse": "Склад",
    "universal": "Универсальное",
    "production": "Производственное",
    "retail": "Торговое",
    "other": "Другое",
}

MANUAL_DEFAULTS = {
    "enabled": True,
    "mode": "manual",
    "entity_role": "competitor",
}


def _ensure_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for path in (MANUAL_COMPETITORS_PATH, MANUAL_RECORDS_PATH, MANUAL_ITEMS_PATH):
        if not path.exists():
            path.write_text("[]", encoding="utf-8")


def _read_json(path: Path) -> List[Dict]:
    _ensure_storage()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        data = []
    return data if isinstance(data, list) else []


def _write_json(path: Path, data: List[Dict]) -> None:
    _ensure_storage()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").replace("\xa0", " ")).strip()


def _slugify(text: str) -> str:
    text = _normalize_name(text).lower().replace("ё", "е")
    text = re.sub(r"[^0-9a-zа-я]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "competitor"


def _parse_datetime(value: str) -> Optional[datetime]:
    value = str(value or "").strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _safe_float(value: object) -> float:
    try:
        return round(float(value or 0), 2)
    except Exception:
        return 0.0


def _checked_date(value: object) -> str:
    dt = _parse_datetime(str(value or ""))
    return dt.strftime("%Y-%m-%d") if dt else ""


def load_manual_competitors() -> List[Dict]:
    result: List[Dict] = []
    for item in _read_json(MANUAL_COMPETITORS_PATH):
        name = _normalize_name(str(item.get("name", "")))
        code = str(item.get("code", "")).strip()
        if not name or not code:
            continue
        result.append({
            "code": code,
            "name": name,
            "short_name": str(item.get("short_name") or name),
            "enabled": bool(item.get("enabled", True)),
            "mode": "manual",
            "entity_role": str(item.get("entity_role") or "competitor"),
        })
    return result


def upsert_manual_competitor(name: str, entity_role: str = "competitor") -> Dict:
    name = _normalize_name(name)
    if not name:
        raise ValueError("Название конкурента не может быть пустым")

    competitors = load_manual_competitors()
    for item in competitors:
        if item["name"].lower() == name.lower():
            changed = False
            if item.get("entity_role") != entity_role:
                item["entity_role"] = entity_role
                changed = True
            if changed:
                _write_json(MANUAL_COMPETITORS_PATH, competitors)
            return item

    base_code = f"manual-{_slugify(name)}"
    code = base_code
    existing_codes = {item["code"] for item in competitors}
    index = 2
    while code in existing_codes:
        code = f"{base_code}-{index}"
        index += 1

    competitor = {
        "code": code,
        "name": name,
        "short_name": name,
        "enabled": True,
        "mode": "manual",
        "entity_role": entity_role,
    }
    competitors.append(competitor)
    _write_json(MANUAL_COMPETITORS_PATH, competitors)
    return competitor


def load_manual_records() -> List[Dict]:
    result: List[Dict] = []
    for item in _read_json(MANUAL_RECORDS_PATH):
        competitor_code = str(item.get("competitor_code", "")).strip()
        competitor_name = _normalize_name(str(item.get("competitor_name", "")))
        if not competitor_code or not competitor_name:
            continue
        row = dict(item)
        row["competitor_code"] = competitor_code
        row["competitor_name"] = competitor_name
        result.append(row)
    return result


def save_manual_record(record: Dict) -> Dict:
    competitor = upsert_manual_competitor(str(record.get("competitor_name", "")))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    free_area = _safe_float(record.get("free_area", 0))
    price_per_sqm = _safe_float(record.get("price_per_sqm", 0))
    total_price = _safe_float(record.get("total_price", 0))
    if total_price <= 0 and free_area > 0 and price_per_sqm > 0:
        total_price = round(free_area * price_per_sqm, 2)

    payload = {
        "competitor_code": competitor["code"],
        "competitor_name": competitor["name"],
        "source": str(record.get("source", "other")),
        "source_label": str(record.get("source_label") or SOURCE_OPTIONS.get(str(record.get("source", "other")), "Другое")),
        "source_url": str(record.get("source_url", "")).strip(),
        "status": str(record.get("status", "free")),
        "status_label": str(record.get("status_label") or STATUS_OPTIONS.get(str(record.get("status", "free")), "Есть свободные помещения")),
        "free_area": free_area,
        "price_per_sqm": price_per_sqm,
        "total_price": total_price,
        "reliability": str(record.get("reliability", "medium")),
        "reliability_label": str(record.get("reliability_label") or RELIABILITY_OPTIONS.get(str(record.get("reliability", "medium")), "Средняя")),
        "comment": str(record.get("comment", "")).strip(),
        "checked_at": str(record.get("checked_at") or now),
    }

    records = load_manual_records()
    records.append(payload)
    _write_json(MANUAL_RECORDS_PATH, records)
    return payload


def get_latest_manual_record(competitor_code: str) -> Optional[Dict]:
    records = [item for item in load_manual_records() if item.get("competitor_code") == competitor_code]
    if not records:
        return None
    records.sort(key=lambda item: str(item.get("checked_at", "")))
    return records[-1]


def list_latest_manual_records() -> List[Dict]:
    latest: Dict[str, Dict] = {}
    for item in load_manual_records():
        code = str(item.get("competitor_code", ""))
        if not code:
            continue
        if code not in latest or str(item.get("checked_at", "")) > str(latest[code].get("checked_at", "")):
            latest[code] = item
    return sorted(latest.values(), key=lambda item: item.get("competitor_name", ""))


def load_manual_items() -> List[Dict]:
    result: List[Dict] = []
    for item in _read_json(MANUAL_ITEMS_PATH):
        competitor_code = str(item.get("competitor_code", "")).strip()
        competitor_name = _normalize_name(str(item.get("competitor_name", "")))
        item_id = str(item.get("id", "")).strip()
        if not competitor_code or not competitor_name or not item_id:
            continue
        row = dict(item)
        row["competitor_code"] = competitor_code
        row["competitor_name"] = competitor_name
        row["id"] = item_id
        row.setdefault("status", "active")
        result.append(row)
    return result


def clear_manual_items_for_competitor(competitor_code: str) -> int:
    """Remove current manual rooms for a competitor from the active manual base.

    Historical dynamics are kept separately in data/history.csv, so this cleanup
    prevents old and new manual rooms from being summed together.
    """
    competitor_code = str(competitor_code or "").strip()
    if not competitor_code:
        return 0
    items = load_manual_items()
    filtered = [item for item in items if str(item.get("competitor_code", "")).strip() != competitor_code]
    deleted = len(items) - len(filtered)
    if deleted:
        _write_json(MANUAL_ITEMS_PATH, filtered)
    return deleted


def save_manual_item(record: Dict) -> Dict:
    competitor_code = str(record.get("competitor_code", "")).strip()
    competitor_name = _normalize_name(str(record.get("competitor_name", "")))
    if competitor_code:
        competitors = {item["code"]: item for item in load_manual_competitors()}
        competitor = competitors.get(competitor_code)
        if competitor is None and competitor_name:
            competitor = upsert_manual_competitor(competitor_name)
        elif competitor is None:
            raise ValueError("Не найден конкурент для помещения")
        competitor_name = competitor["name"]
        competitor_code = competitor["code"]
    else:
        competitor = upsert_manual_competitor(competitor_name)
        competitor_name = competitor["name"]
        competitor_code = competitor["code"]

    title = _normalize_name(str(record.get("title", "")))
    if not title:
        raise ValueError("Название помещения не может быть пустым")

    room_type = str(record.get("type", "other"))
    type_label = str(record.get("type_label") or ROOM_TYPE_OPTIONS.get(room_type, "Другое"))
    source = str(record.get("source", "other"))
    source_label = str(record.get("source_label") or SOURCE_OPTIONS.get(source, "Другое"))
    reliability = str(record.get("reliability", "medium"))
    reliability_label = str(record.get("reliability_label") or RELIABILITY_OPTIONS.get(reliability, "Средняя"))

    area = _safe_float(record.get("area", 0))
    price_per_sqm = _safe_float(record.get("price_per_sqm", 0))
    total_price = _safe_float(record.get("total_price", 0))
    if total_price <= 0 and area > 0 and price_per_sqm > 0:
        total_price = round(area * price_per_sqm, 2)

    checked_at_dt = _parse_datetime(str(record.get("checked_at") or "")) or datetime.now()
    checked_at = checked_at_dt.strftime("%Y-%m-%d %H:%M:%S")
    checked_day = checked_at_dt.strftime("%Y-%m-%d")
    review_due_at = (checked_at_dt + timedelta(days=MANUAL_REVIEW_DAYS)).strftime("%Y-%m-%d")
    expire_at = (checked_at_dt + timedelta(days=MANUAL_EXPIRE_DAYS)).strftime("%Y-%m-%d")

    items = load_manual_items()
    old_items = [item for item in items if item.get("competitor_code") == competitor_code]
    has_same_day_batch = any(_checked_date(item.get("checked_at")) == checked_day for item in old_items)
    replace_existing = bool(record.get("replace_existing") or record.get("start_new_revision"))

    # Final business rule: a manual competitor has only one current revision.
    # When the first room of a new check date is added, old current rooms are
    # removed so they are not summed with the new actual list.
    if old_items and (replace_existing or not has_same_day_batch):
        items = [item for item in items if item.get("competitor_code") != competitor_code]

    payload = {
        "id": str(record.get("id") or uuid4().hex[:12]),
        "competitor_code": competitor_code,
        "competitor_name": competitor_name,
        "title": title,
        "type": room_type,
        "type_label": type_label,
        "area": area,
        "price_per_sqm": price_per_sqm,
        "total_price": total_price,
        "source": source,
        "source_label": source_label,
        "source_url": str(record.get("source_url", "")).strip(),
        "reliability": reliability,
        "reliability_label": reliability_label,
        "comment": str(record.get("comment", "")).strip(),
        "checked_at": checked_at,
        "review_due_at": review_due_at,
        "expire_at": expire_at,
        "status": str(record.get("status") or "active"),
    }

    items.append(payload)
    _write_json(MANUAL_ITEMS_PATH, items)
    return payload


def delete_manual_item(item_id: str) -> bool:
    item_id = str(item_id or "").strip()
    if not item_id:
        return False
    items = load_manual_items()
    filtered = [item for item in items if str(item.get("id", "")).strip() != item_id]
    if len(filtered) == len(items):
        return False
    _write_json(MANUAL_ITEMS_PATH, filtered)
    return True


def _decorate_manual_item(item: Dict) -> Dict:
    row = dict(item)
    checked_at_dt = _parse_datetime(str(item.get("checked_at") or ""))
    age_days = 999
    if checked_at_dt:
        age_days = max((datetime.now().date() - checked_at_dt.date()).days, 0)
    row["age_days"] = age_days
    if age_days >= MANUAL_EXPIRE_DAYS:
        row["review_status"] = "expired"
        row["review_status_label"] = "Истек срок без подтверждения"
        row["is_active"] = False
    elif age_days >= MANUAL_REVIEW_DAYS:
        row["review_status"] = "review"
        row["review_status_label"] = "Нужен обзвон / проверка"
        row["is_active"] = True
    else:
        row["review_status"] = "fresh"
        row["review_status_label"] = "Свежая запись"
        row["is_active"] = True
    return row


def list_manual_items_for_competitor(competitor_code: str, include_inactive: bool = True) -> List[Dict]:
    rows = [_decorate_manual_item(item) for item in load_manual_items() if item.get("competitor_code") == competitor_code]
    if not include_inactive:
        rows = [item for item in rows if item.get("is_active")]
    rows.sort(key=lambda item: (str(item.get("checked_at", "")), str(item.get("title", "")).lower()), reverse=True)
    return rows


def build_items_from_manual_items(competitor_code: str) -> Dict:
    rows = list_manual_items_for_competitor(competitor_code, include_inactive=True)
    active_items: List[Dict] = []
    review_items: List[Dict] = []
    expired_items: List[Dict] = []
    latest_checked = ""

    for row in rows:
        checked_at = str(row.get("checked_at", ""))
        if checked_at > latest_checked:
            latest_checked = checked_at
        item = {
            "item_key": f"manualroom:{row['id']}",
            "company": row.get("competitor_name", ""),
            "type": row.get("type_label", row.get("type", "Другое")),
            "title": row.get("title", "Ручное помещение"),
            "area": _safe_float(row.get("area", 0)),
            "price_value": _safe_float(row.get("price_per_sqm", 0)),
            "price_per_sqm": _safe_float(row.get("price_per_sqm", 0)),
            "total_price_value": _safe_float(row.get("total_price", 0)),
            "total_price": _safe_float(row.get("total_price", 0)),
            "url": row.get("source_url", ""),
            "source_url": row.get("source_url", ""),
            "source_label": row.get("source_label", "Другое"),
            "reliability_label": row.get("reliability_label", "Средняя"),
            "checked_at": row.get("checked_at", ""),
            "comment": row.get("comment", ""),
        }
        if row.get("review_status") == "expired":
            expired_items.append(item)
        else:
            active_items.append(item)
            if row.get("review_status") == "review":
                review_items.append(item)

    return {
        "items": active_items,
        "review_items": review_items,
        "expired_items": expired_items,
        "expired_item_keys": [item["item_key"] for item in expired_items],
        "latest_checked_at": latest_checked,
    }


def get_latest_manual_timestamp(competitor_code: str) -> str:
    latest = ""
    record = get_latest_manual_record(competitor_code)
    if record and str(record.get("checked_at", "")) > latest:
        latest = str(record.get("checked_at", ""))
    bundle = build_items_from_manual_items(competitor_code)
    if bundle.get("latest_checked_at", "") > latest:
        latest = str(bundle.get("latest_checked_at", ""))
    return latest


def delete_manual_competitor_data(competitor_code: str) -> Dict[str, int]:
    competitor_code = str(competitor_code or '').strip()
    if not competitor_code:
        return {'deleted_records': 0, 'deleted_competitors': 0, 'deleted_items': 0}

    competitors = load_manual_competitors()
    filtered_competitors = [item for item in competitors if str(item.get('code', '')).strip() != competitor_code]
    deleted_competitors = len(competitors) - len(filtered_competitors)
    _write_json(MANUAL_COMPETITORS_PATH, filtered_competitors)

    records = load_manual_records()
    filtered_records = [item for item in records if str(item.get('competitor_code', '')).strip() != competitor_code]
    deleted_records = len(records) - len(filtered_records)
    _write_json(MANUAL_RECORDS_PATH, filtered_records)

    deleted_items = clear_manual_items_for_competitor(competitor_code)
    return {'deleted_records': deleted_records, 'deleted_competitors': deleted_competitors, 'deleted_items': deleted_items}


def list_manual_competitors_with_records() -> List[Dict]:
    competitors_by_code = {item['code']: item for item in load_manual_competitors()}
    latest_records = {item['competitor_code']: item for item in list_latest_manual_records()}
    result: List[Dict] = []

    for code, competitor in competitors_by_code.items():
        row = dict(competitor)
        row['latest_record'] = latest_records.get(code)
        room_bundle = build_items_from_manual_items(code)
        row['room_count'] = len(room_bundle.get('items') or [])
        row['latest_checked_at'] = get_latest_manual_timestamp(code)
        result.append(row)

    result.sort(key=lambda item: str(item.get('name', '')).lower())
    return result


def list_manual_review_rows() -> List[Dict]:
    rows = []
    for item in load_manual_items():
        decorated = _decorate_manual_item(item)
        if decorated.get("review_status") == "fresh":
            continue
        rows.append({
            "item_id": decorated.get("id", ""),
            "Конкурент": decorated.get("competitor_name", ""),
            "Помещение": decorated.get("title", ""),
            "Тип": decorated.get("type_label", decorated.get("type", "")),
            "Возраст, дней": decorated.get("age_days", 0),
            "Статус": decorated.get("review_status_label", ""),
            "Площадь, м²": _safe_float(decorated.get("area", 0)),
            "Ставка за м², ₽": _safe_float(decorated.get("price_per_sqm", 0)),
            "Общая стоимость, ₽": _safe_float(decorated.get("total_price", 0)),
            "Источник": decorated.get("source_label", ""),
            "Последняя проверка": decorated.get("checked_at", ""),
            "Ссылка": decorated.get("source_url", ""),
        })
    rows.sort(key=lambda row: (row.get("Возраст, дней", 0), row.get("Конкурент", "")), reverse=True)
    return rows


def build_items_from_manual_record(record: Optional[Dict]) -> List[Dict]:
    if not record:
        return []

    status = str(record.get("status", "free"))
    if status != "free":
        return []

    area = _safe_float(record.get("free_area", 0))
    price_per_sqm = _safe_float(record.get("price_per_sqm", 0))
    total_price = _safe_float(record.get("total_price", 0))
    if total_price <= 0 and area > 0 and price_per_sqm > 0:
        total_price = round(area * price_per_sqm, 2)

    title = str(record.get("comment") or "Ручная запись").strip() or "Ручная запись"
    return [{
        "company": record.get("competitor_name", ""),
        "type": "Ручной учет",
        "title": title,
        "area": round(area, 2),
        "price_value": round(price_per_sqm, 2),
        "price_per_sqm": round(price_per_sqm, 2),
        "total_price_value": round(total_price, 2),
        "total_price": round(total_price, 2),
        "url": record.get("source_url", ""),
        "source_url": record.get("source_url", ""),
        "source_label": record.get("source_label", "Другое"),
        "reliability_label": record.get("reliability_label", "Средняя"),
        "checked_at": record.get("checked_at", ""),
    }]
