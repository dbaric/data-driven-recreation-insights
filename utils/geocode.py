"""
Geocode address strings to lat/lng via Nominatim.
Uses data/cache/geocode_cache.json for persistent cache.
"""

import json
import re
import time
import urllib.parse
from pathlib import Path

USER_AGENT = "unist-sport/1.0"
RATE_LIMIT_SEC = 1.1

_LAST_REQUEST_TIME = 0.0


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent  # utils/ -> project root


def cache_path() -> Path:
    return _project_root() / "data" / "cache" / "geocode_cache.json"


def _ensure_cache_dir() -> None:
    cache_path().parent.mkdir(parents=True, exist_ok=True)


def _migrate_legacy_cache() -> None:
    """Copy residence_lat_lng.json to geocode_cache.json if geocode_cache doesn't exist."""
    legacy = _project_root() / "data" / "cache" / "residence_lat_lng.json"
    cache = cache_path()
    if legacy.exists() and not cache.exists():
        _ensure_cache_dir()
        with open(legacy, encoding="utf-8") as f:
            data = json.load(f)
        with open(cache, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


COUNTRY_NAMES = {
    "HR": "Croatia",
    "BA": "Bosnia and Herzegovina",
    "ME": "Montenegro",
    "IT": "Italy",
    "LT": "Lithuania",
    "KZ": "Kazakhstan",
    "PL": "Poland",
    "SE": "Sweden",
    "MK": "North Macedonia",
    "AE": "United Arab Emirates",
    "SI": "Slovenia",
    "RS": "Serbia",
    "HU": "Hungary",
    "AT": "Austria",
    "DE": "Germany",
    "SK": "Slovakia",
    "CZ": "Czech Republic",
}


def build_query(address: str | None, country_code: str | None) -> str | None:
    cc = (country_code or "").upper()
    if not cc or len(cc) != 2:
        return None
    country_name = COUNTRY_NAMES.get(cc, cc)
    q = str(address).strip() if address else ""
    if not q:
        return None
    return f"{q}, {country_name}" if not q.endswith(country_name) else q


def _fallback_queries(cache_key: str) -> list[str]:
    """Generate simpler queries to try when full address returns null."""
    if not cache_key or ", " not in cache_key:
        return []
    parts = [p.strip() for p in cache_key.split(", ")]
    if len(parts) < 2:
        return []
    country = parts[-1]
    fallbacks = []
    if len(parts) >= 4:
        fallbacks.append(", ".join([parts[0], parts[1], parts[-1]]))
    if len(parts) >= 3:
        fallbacks.append(", ".join(parts[-3:]))
    if len(parts) >= 2:
        fallbacks.append(", ".join(parts[-2:]))
    if len(parts) >= 3:
        address_part = parts[0]
        street_no_strip = re.sub(r"\s*\d+[a-zA-Z]?\s*$", "", address_part).strip()
        if street_no_strip and street_no_strip != address_part:
            fallbacks.append(f"{street_no_strip}, {parts[1]}, {country}")
    return [q for q in fallbacks if q and q != cache_key]


CAMPUS_SPLIT_CANONICAL = "Cvite Fiskovića 3, Split, Croatia"

CACHE_QUERY_CORRECTIONS = {
    "Velika dvorana, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Velika dvorana - studentski dom KAMPUS, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Velika dvorana - KAMPUS Studentski dom dr. Franje Tuđmana 3, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Studentski dom dr. Franje Tuđmana 3, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Studentski dom Kampus, Split, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Multifunkcionalna dvorana Kampus (ispod tribine). Studentski dom dr. Franjo Tuđman Cvite Fiskovića 3, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Multifunkcionalna dvorana Kampus (ispod tribine). Studentski dom dr. Franjo Tuđman Cvite Fiskovića 3, Split, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Kampus . Studentski dom dr. Franjo Tuđman Cvite Fiskovića 3, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Kampus . Studentski dom dr. Franjo Tuđman Cvite Fiskovića 3, Split, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Multifunkcionalna dvorana Kampus (ispod tribine). Studentski dom Kampus dr. Franje Tuđmana Cvite Fiskovića 3, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Multifunkcionalna dvorana Kampus (ispod tribine). Studentski dom Kampus dr. Franje Tuđmana Cvite Fiskovića 3, Split, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Kampus . Studentski dom Kampus dr. Franje Tuđmana Cvite Fiskovića 3, Croatia": CAMPUS_SPLIT_CANONICAL,
    "Kampus . Studentski dom Kampus dr. Franje Tuđmana Cvite Fiskovića 3, Split, Croatia": CAMPUS_SPLIT_CANONICAL,
    "SPINUT FUTSAL TEREN, Split, Croatia": "SPINUT FUTSAL TEREN, Croatia",
    "Spinut futsal teren, Split, Croatia": "Spinut futsal teren, Croatia",
}

KEYS_TO_PURGE_FROM_CACHE = frozenset(CACHE_QUERY_CORRECTIONS)


def _load_cache() -> dict:
    _migrate_legacy_cache()
    path = cache_path()
    if path.exists():
        with open(path, encoding="utf-8") as f:
            try:
                data = json.load(f)
                purged = False
                for key in KEYS_TO_PURGE_FROM_CACHE:
                    if key in data:
                        del data[key]
                        purged = True
                if purged:
                    _save_cache(data)
                return data
            except json.JSONDecodeError:
                return {}
    return {}


def _save_cache(cache: dict) -> None:
    _ensure_cache_dir()
    with open(cache_path(), "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _fetch_nominatim(query: str, country_code: str) -> tuple[str | None, str | None]:
    global _LAST_REQUEST_TIME
    params = {"q": query, "format": "json", "limit": 1}
    cc = (country_code or "").upper()
    if cc and len(cc) == 2:
        params["countrycodes"] = cc.lower()
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(params)
    import requests

    for _ in range(3):
        elapsed = time.time() - _LAST_REQUEST_TIME
        if elapsed < RATE_LIMIT_SEC:
            time.sleep(RATE_LIMIT_SEC - elapsed)
        _LAST_REQUEST_TIME = time.time()
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        data = resp.json()
        if data and len(data) > 0:
            lat, lon = data[0].get("lat"), data[0].get("lon")
            return (lat, lon) if lat and lon else (None, None)
        return (None, None)


def geocode_address(
    address: str | None,
    country_code: str | None = "HR",
    *,
    skip_api: bool = False,
    fallbacks: bool = True,
) -> tuple[float | None, float | None]:
    """
    Geocode address to (lat, lng). Uses cache, then Nominatim (unless skip_api).
    Returns (lat, lng) or (None, None).
    """
    query = build_query(address, country_code)
    if not query:
        return None, None
    query = CACHE_QUERY_CORRECTIONS.get(query, query)

    cc = (country_code or "").upper()
    cache = _load_cache()

    if query in cache:
        val = cache[query]
        if val is not None:
            lat, lng = val.get("lat"), val.get("lng")
            return (float(lat), float(lng)) if lat and lng else (None, None)
        if skip_api:
            return None, None
        if fallbacks:
            for fallback in _fallback_queries(query):
                lat, lng = _fetch_nominatim(fallback, cc)
                if lat and lng:
                    cache[query] = {"lat": lat, "lng": lng}
                    _save_cache(cache)
                    return float(lat), float(lng)
        return None, None

    if skip_api:
        return None, None

    lat, lng = _fetch_nominatim(query, cc)
    if lat and lng:
        cache = _load_cache()
        cache[query] = {"lat": lat, "lng": lng}
        _save_cache(cache)
        return float(lat), float(lng)

    if fallbacks:
        for fallback in _fallback_queries(query):
            lat, lng = _fetch_nominatim(fallback, cc)
            if lat and lng:
                cache = _load_cache()
                cache[query] = {"lat": lat, "lng": lng}
                _save_cache(cache)
                return float(lat), float(lng)

    cache = _load_cache()
    cache[query] = None
    _save_cache(cache)
    return None, None


def normalize_location(location: str) -> str:
    """Normalize event location string for geocoding (replace newlines, strip)."""
    if not location or not str(location).strip():
        return ""
    return " ".join(str(location).replace("\n", " ").split())


EVENT_VENUE_PREFIXES = (
    r"^(?:Judo klub|Velika dvorana|Mala dvorana|Multifunkcionalna dvorana|"
    r"Hrvačka dvorana|ŠC BAZENI|SPINUT|VELIKA DVORANA|Mala sportska)[\s\-]*",
    r"[\s\-]*(?:futsal|košarkaški|sport)[\s\/]*(?:teren|dvorana)?[\s\-]*",
    r"[\s\-]*(?:ispod tribine|u sklopu teretane)\.?",
    r"[\s\-]*(?:Meet point|PK MARULIANUS|VK Gusar)[\s:]*",
    r"\([^)]*\)",  # remove parentheticals like "(ispod tribine)"
)
KNOWN_SPLIT_PLACES = ("Split", "Pujanke", "Spinut", "Poljud", "Žnjan", "Kampus")
GENERIC_WORDS_BLOCKLIST = frozenset(
    {"dvorana", "teren", "kampus", "split", "dvorane", "škola", "dom", "ispred"}
)
EVENT_PART_BLOCKLIST = frozenset(
    {"velika dvorana", "mala dvorana", "velika dvorana - kampus", "velika dvorana - studentski dom kampus"}
)


def event_location_fallbacks(location: str, country_code: str = "HR") -> list[str]:
    """
    Extract fallback queries from event location strings using generic rules.
    Event locations often mix venue names with addresses; this extracts geocodable parts.
    """
    loc = normalize_location(location)
    if not loc:
        return []
    country = COUNTRY_NAMES.get(country_code, "Croatia")
    if loc.endswith(f", {country}"):
        loc = loc[: -len(country) - 2].strip()
    fallbacks = []
    seen: set[str] = set()

    def add(q: str) -> None:
        q = q.strip()
        if not q or len(q) < 5:
            return
        if q.replace(",", "").replace(".", "").strip().isdigit():
            return
        if not q.endswith(country):
            q = f"{q}, {country}"
        if q not in seen:
            fallbacks.append(q)
            seen.add(q)

    def add_with_split(q: str) -> None:
        if q.endswith(", Split") or ", Split" in q:
            add(q)
        else:
            add(f"{q}, Split")

    parts_by_dash = [p.strip() for p in re.split(r"\s+-\s+", loc, maxsplit=2)]
    for part in parts_by_dash:
        if not part or len(part) < 3:
            continue
        part_lower = part.lower()
        if part_lower in EVENT_PART_BLOCKLIST or any(
            part_lower.startswith(p) for p in ("velika dvorana", "mala dvorana")
        ):
            add("Studentski dom Kampus, Split")
            add("Cvite Fiskovića 3, Split")
            continue
        add_with_split(part)
        words = part.split()
        if words:
            last_word = words[-1].rstrip(".,")
            if (
                last_word in KNOWN_SPLIT_PLACES
                or (last_word.isalpha() and last_word.lower() not in GENERIC_WORDS_BLOCKLIST)
            ):
                add_with_split(last_word)

    parts_by_comma = [p.strip() for p in loc.split(",")]
    for i in range(len(parts_by_comma)):
        suffix = ", ".join(parts_by_comma[-(i + 1) :]) if i < len(parts_by_comma) else parts_by_comma[-1]
        if suffix and len(suffix) > 2:
            add_with_split(suffix)

    cleaned = loc
    for pat in EVENT_VENUE_PREFIXES:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE)
    cleaned = " ".join(cleaned.split())
    if cleaned and cleaned != loc and len(cleaned) > 3:
        add_with_split(cleaned)
        words = cleaned.split()
        if words:
            lw = words[-1].rstrip(".,")
            if lw.lower() not in GENERIC_WORDS_BLOCKLIST and not lw.isdigit():
                add_with_split(lw)

    for place in KNOWN_SPLIT_PLACES:
        if place in loc and place != "Split":
            add_with_split(place)

    if re.search(r"\b[Cc]vite Fiskovića\b", loc):
        m = re.search(r"(Cvite Fiskovića\s*\d*)", loc, re.I)
        if m:
            add(f"{m.group(1).strip()}, Split")
        add("Cvite Fiskovića 3, Split")

    if "studentskog doma" in loc.lower():
        m = re.search(r"studentskog doma\s+([^,\-]+)", loc, re.I)
        if m:
            add(f"Studentski dom {m.group(1).strip()}, Split")

    if "Osmih mediteranskih" in loc or "mediteranskih igara" in loc.lower():
        m = re.search(r"([IVX]+\.?\s*)?Osmih?\s*mediteranskih\s+igara\s*(\d+)", loc, re.I)
        if m:
            add(f"Osmih mediteranskih igara {m.group(2)}, Split")
        add("Osmih mediteranskih igara 21, Split")

    if re.search(r"Plančićeva", loc, re.I):
        m = re.search(r"Plančićeva\s+(?:ul\.?\s*)?(\d+)", loc, re.I)
        if m:
            add(f"Plančićeva {m.group(1)}, Split")

    if "Šetalište" in loc or "Pape" in loc:
        add("Šetalište Pape Ivana Pavla II, Split")

    add("Split, Croatia")

    return [q for q in fallbacks if not re.match(r"^Split, Split, Croatia$", q)]


def geocode_event_location(
    location: str | None,
    country_code: str = "HR",
    *,
    skip_api: bool = False,
) -> tuple[float | None, float | None]:
    """
    Geocode event location with event-specific fallbacks.
    Tries main query first, then event_location_fallbacks.
    """
    addr = normalize_location(location) if location else ""
    if not addr:
        return None, None

    result = geocode_address(addr, country_code, skip_api=skip_api, fallbacks=True)
    if result[0] is not None and result[1] is not None:
        return result

    if skip_api:
        return None, None

    orig_key = build_query(addr, country_code)
    for fallback in event_location_fallbacks(addr, country_code):
        fallback_key = build_query(fallback, country_code)
        if not fallback_key or fallback_key == orig_key:
            continue
        result = geocode_address(fallback, country_code, skip_api=False, fallbacks=False)
        if result[0] is not None and result[1] is not None:
            if orig_key:
                cache = _load_cache()
                cache[orig_key] = {"lat": result[0], "lng": result[1]}
                _save_cache(cache)
            return result

    return None, None


def add_lat_lng_for_column(
    df: "pd.DataFrame",
    column: str,
    country_code: str = "HR",
    *,
    skip_api: bool = False,
    use_event_fallbacks: bool = False,
) -> "pd.DataFrame":
    """Add lat, lng columns from geocoding the given address column. Returns new DataFrame."""
    import pandas as pd

    unique_addrs = df[column].dropna().apply(normalize_location).unique()
    unique_addrs = [a for a in unique_addrs if a]

    def do_geocode(addr):
        if use_event_fallbacks:
            return geocode_event_location(addr, country_code, skip_api=skip_api)
        return geocode_address(addr, country_code, skip_api=skip_api, fallbacks=True)

    coords = {}
    for addr in unique_addrs:
        key = build_query(addr, country_code)
        if key and key not in coords:
            coords[key] = do_geocode(addr)

    def get_lat_lng(val):
        addr = normalize_location(val) if pd.notna(val) else ""
        if not addr:
            return None, None
        key = build_query(addr, country_code)
        return coords.get(key, (None, None))

    def to_float(v):
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    lat_lng = df[column].apply(get_lat_lng)
    out = df.copy()
    out["lat"] = lat_lng.apply(lambda x: to_float(x[0]))
    out["lng"] = lat_lng.apply(lambda x: to_float(x[1]))
    return out

