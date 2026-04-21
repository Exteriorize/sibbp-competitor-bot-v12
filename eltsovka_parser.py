from __future__ import annotations

import re
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from sib_parser import ParserError


BASE_URL = "https://eltsovka-1.ru/services/commercial-eltsovka-1/"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}
LISTING_SKIP_TEXT = {"подробнее", "заказать", "посмотреть", "возврат к списку"}
TYPE_BY_SEGMENT = {
    "office-space": "Офис",
    "warehouse": "Склад",
    "industrial-premises": "Производственное",
}
_NUMBER_RE = r"(\d[\d\s]{0,20}(?:[.,]\d+)?)"


def _normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\xa0", " ")).strip()


def _to_float(raw: str) -> float:
    return float(_normalize_spaces(raw).replace(" ", "").replace(",", "."))


def _safe_get(session: requests.Session, url: str) -> requests.Response:
    response = session.get(url, headers=DEFAULT_HEADERS, timeout=30)
    response.raise_for_status()
    return response


def _build_page_url(base_url: str, page_param: str, page: int) -> str:
    if page <= 1:
        return base_url
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query[page_param] = [str(page)]
    parts = [f"{key}={value}" for key, values in query.items() for value in values]
    return urlunparse(parsed._replace(query="&".join(parts)))


def _is_detail_url(url: str) -> bool:
    parsed = urlparse(url)
    path_parts = [part for part in parsed.path.split("/") if part]
    if parsed.netloc and "eltsovka-1.ru" not in parsed.netloc:
        return False
    if len(path_parts) < 4:
        return False
    if path_parts[0] != "services" or path_parts[1] != "commercial-eltsovka-1":
        return False
    return True


def _looks_like_listing_title(text: str) -> bool:
    text_norm = _normalize_spaces(text).lower()
    if not text_norm or text_norm in LISTING_SKIP_TEXT:
        return False
    if "кв" in text_norm or "м²" in text_norm or "м2" in text_norm:
        return True
    return any(keyword in text_norm for keyword in ("офис", "склад", "помещ", "производств", "общепит"))


def _extract_pagination(soup: BeautifulSoup) -> Tuple[str, int]:
    max_page = 1
    page_param = "PAGEN_2"

    for a in soup.find_all("a", href=True):
        href = urljoin(BASE_URL, a["href"])
        parsed = urlparse(href)
        if parsed.path.rstrip("/") != urlparse(BASE_URL).path.rstrip("/"):
            continue
        query = parse_qs(parsed.query, keep_blank_values=True)
        for key, values in query.items():
            if not key.startswith("PAGEN_"):
                continue
            page_param = key
            for value in values:
                if str(value).isdigit():
                    max_page = max(max_page, int(value))

    for text in soup.stripped_strings:
        cleaned = _normalize_spaces(text)
        if cleaned.isdigit():
            max_page = max(max_page, int(cleaned))

    return page_param, max_page


def _extract_listing_links(soup: BeautifulSoup) -> List[str]:
    links: List[str] = []
    seen: Set[str] = set()
    for a in soup.find_all("a", href=True):
        href = urljoin(BASE_URL, a["href"])
        if not _is_detail_url(href):
            continue
        if urlparse(href).query:
            continue
        text = _normalize_spaces(a.get_text(" ", strip=True)) or _normalize_spaces(a.get("title") or "")
        if not _looks_like_listing_title(text):
            continue
        if href not in seen:
            seen.add(href)
            links.append(href)
    return links


def _extract_title(soup: BeautifulSoup) -> str:
    for selector in ("h1", "h2", ".entry-title", ".page-title"):
        node = soup.select_one(selector)
        if node:
            text = _normalize_spaces(node.get_text(" ", strip=True))
            if text:
                return text
    return ""


def _extract_first_number(text: str, patterns: List[str], *, min_value: float = 0.0, max_value: float = 1e12) -> float:
    text_norm = _normalize_spaces(text)
    for pattern in patterns:
        for match in re.finditer(pattern, text_norm, flags=re.IGNORECASE):
            try:
                value = _to_float(match.group(1))
            except Exception:
                continue
            if min_value <= value <= max_value:
                return round(value, 2)
    return 0.0


def _extract_area(text: str, title: str) -> float:
    patterns = [
        rf"Площадь\s*,?\s*кв\.?\s*м\.?\s*{_NUMBER_RE}",
        rf"общей\s+площад(?:ью|и)\s*{_NUMBER_RE}\s*(?:кв\.?\s*м|м²|м2)",
        rf"площадью\s*{_NUMBER_RE}\s*(?:кв\.?\s*м|м²|м2)",
        rf"{_NUMBER_RE}\s*(?:кв\.?\s*м|м²|м2)",
    ]
    area = _extract_first_number(text, patterns, min_value=1, max_value=100000)
    if area > 0:
        return area
    return _extract_first_number(title, patterns[-1:], min_value=1, max_value=100000)


def _extract_monthly_price(text: str) -> float:
    patterns = [
        rf"Цена\s+за\s+месяц\s*,?\s*руб\.?\s*{_NUMBER_RE}",
        rf"Цена\s+за\s+месяц[^\d]{{0,20}}{_NUMBER_RE}",
    ]
    return _extract_first_number(text, patterns, min_value=1, max_value=100000000)


def _extract_rate(text: str) -> float:
    patterns = [
        rf"Цена\s+за\s*1\s*м\s*\^?\s*\{{?2\}}?\s*,?\s*руб\.?\s*{_NUMBER_RE}",
        rf"Цена\s+за\s*1\s*м\s*2\s*,?\s*руб\.?\s*{_NUMBER_RE}",
        rf"Ставка\s+арендной\s+платы\s+составляет\s*{_NUMBER_RE}\s*(?:руб|рублей)",
        rf"{_NUMBER_RE}\s*(?:руб|рублей)\s+за\s+один\s+кв\.?\s*м",
    ]
    return _extract_first_number(text, patterns, min_value=1, max_value=1000000)


def _detect_type(url: str, title: str) -> str:
    parts = [part for part in urlparse(url).path.split("/") if part]
    segment = parts[2] if len(parts) >= 3 else ""
    if segment in TYPE_BY_SEGMENT:
        return TYPE_BY_SEGMENT[segment]
    title_norm = _normalize_spaces(title).lower()
    if "склад" in title_norm:
        return "Склад"
    if "производ" in title_norm:
        return "Производственное"
    return "Офис"


def _parse_detail(session: requests.Session, url: str) -> Optional[Dict]:
    response = _safe_get(session, url)
    soup = BeautifulSoup(response.text, "html.parser")
    title = _extract_title(soup)
    if not title:
        return None

    text = _normalize_spaces(soup.get_text(" ", strip=True))
    area = _extract_area(text, title)
    monthly_price = _extract_monthly_price(text)
    rate = _extract_rate(text)

    if area > 0 and monthly_price > 0 and rate <= 0:
        rate = round(monthly_price / area, 2)
    if area > 0 and rate > 0 and monthly_price <= 0:
        monthly_price = round(area * rate, 2)

    room_type = _detect_type(url, title)
    item_key = f"eltsovka1|{urlparse(url).path.rstrip('/')}"

    return {
        "item_key": item_key,
        "title": title,
        "type": room_type,
        "area": area,
        "price_value": rate,
        "price_per_sqm": rate,
        "total_price_value": monthly_price,
        "total_price": monthly_price,
        "url": url,
        "source_url": url,
    }


def parse_eltsovka1() -> List[Dict]:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    first_page = _safe_get(session, BASE_URL)
    first_soup = BeautifulSoup(first_page.text, "html.parser")
    page_param, max_page = _extract_pagination(first_soup)

    detail_urls: List[str] = []
    seen_urls: Set[str] = set()

    for page in range(1, max_page + 1):
        if page == 1:
            soup = first_soup
        else:
            page_url = _build_page_url(BASE_URL, page_param, page)
            soup = BeautifulSoup(_safe_get(session, page_url).text, "html.parser")

        for url in _extract_listing_links(soup):
            if url not in seen_urls:
                seen_urls.add(url)
                detail_urls.append(url)

    items: List[Dict] = []
    for url in detail_urls:
        try:
            item = _parse_detail(session, url)
        except requests.RequestException:
            continue
        if not item:
            continue
        if item.get("area") and item.get("area") > 0:
            items.append(item)

    if not items:
        raise ParserError("Не удалось получить помещения с сайта Ельцовка-1")

    return items
