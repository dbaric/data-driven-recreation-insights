"""
Parse dirtySocks HTML from User.dirtySocks into structured fields.
"""

import html
import re

import pandas as pd

LABEL_TO_KEY = {
    "Prebivalište:": "dirtySocks_prebivaliste",
    "Boravište:": "dirtySocks_boraviste",
    "Državljanstvo:": "dirtySocks_drzavljanstvo",
    "OIB:": "dirtySocks_oib",
    "JMBAG:": "dirtySocks_jmbag",
    "Europski Studentski Identifikator (ESI):": "dirtySocks_esi",
    "Datum rođenja:": "dirtySocks_datum_rodjenja",
    "Telefon:": "dirtySocks_telefon",
}


def parse_dirty_socks(html_str: str) -> dict:
    """
    Parsira HTML iz stupca dirtySocks u dict s poljima:
    dirtySocks_prebivaliste, dirtySocks_boraviste, ...
    """
    empty = {
        "dirtySocks_prebivaliste": None,
        "dirtySocks_boraviste": None,
        "dirtySocks_drzavljanstvo": None,
        "dirtySocks_oib": None,
        "dirtySocks_jmbag": None,
        "dirtySocks_esi": None,
        "dirtySocks_datum_rodjenja": None,
        "dirtySocks_telefon": None,
    }
    if pd.isna(html_str) or not str(html_str).strip():
        return empty

    result = empty.copy()
    pattern = r"<span[^>]*>([^<]+)</span>\s*([^<]*)"
    for label, value in re.findall(pattern, html_str):
        label = html.unescape(label).strip()
        value = html.unescape(value).strip() or None
        key = LABEL_TO_KEY.get(label)
        if key:
            result[key] = value
    return result


def to_iso_date(val) -> str | None:
    """DD.MM.YYYY -> YYYY-MM-DD"""
    if pd.isna(val) or not str(val).strip():
        return None
    parts = str(val).strip().split(".")
    if len(parts) != 3:
        return None
    d, m, y = parts[0].zfill(2), parts[1].zfill(2), parts[2]
    return f"{y}-{m}-{d}" if len(y) == 4 else None


def extract_country_code_and_address(val) -> tuple[str | None, str | None]:
    """Extract (HR) or (BA) from residence, return (address_clean, country_code)."""
    if pd.isna(val) or not str(val).strip():
        return None, None
    s = str(val).strip()
    match = re.search(r"\(([A-Z]{2})\)", s)
    if match:
        code = match.group(1)
        address_clean = re.sub(r"\s*\([A-Z]{2}\)\s*,\s*", ", ", s)
        address_clean = re.sub(r"\s*\([A-Z]{2}\)\s*", " ", address_clean)
        address_clean = re.sub(r"\s+", " ", address_clean).strip()
        return address_clean, code
    return s, None
