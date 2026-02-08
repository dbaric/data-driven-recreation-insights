"""
Geocode residence strings to lat/lng.
Uses utils.geocode (Nominatim, data/cache/geocode_cache.json).
"""

import pandas as pd

from utils.geocode import build_query, geocode_address


def _address_and_country(row: pd.Series) -> tuple[str | None, str | None]:
    """Return (address, country_code). Prefer residence; fallback to placeOfBirth with HR."""
    r = row.get("residence")
    if pd.notna(r) and str(r).strip():
        return str(r).strip(), (row.get("country_code") or "").upper() or None
    pob = row.get("placeOfBirth")
    if pd.notna(pob) and str(pob).strip():
        return str(pob).strip(), "HR"
    return None, None


def geocode_lat_lng(
    residence, country_code: str | None, *, skip_api: bool = False
) -> tuple[float | None, float | None]:
    """Returns (lat, lng) or (None, None). Uses cache, then Nominatim (unless skip_api)."""
    return geocode_address(residence, country_code, skip_api=skip_api, fallbacks=True)


def add_lat_lng_to_df(df: pd.DataFrame, *, skip_api: bool = False) -> pd.DataFrame:
    """
    Add lat, lng columns to dataframe.
    Uses residence when available; falls back to placeOfBirth (with HR) when residence is empty.
    When skip_api=True, only uses cache; returns None for cache misses.
    """
    unique_pairs = set()
    for _, row in df.iterrows():
        addr, cc = _address_and_country(row)
        if addr and cc:
            unique_pairs.add((addr, cc))

    coords = {}
    for addr, cc in unique_pairs:
        key = build_query(addr, cc)
        if key and key not in coords:
            coords[key] = geocode_address(addr, cc, skip_api=skip_api, fallbacks=True)

    def get_lat_lng(row):
        addr, cc = _address_and_country(row)
        if not addr or not cc:
            return None, None
        key = build_query(addr, cc)
        return coords.get(key, (None, None))

    def to_float(val):
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    lat_lng = df.apply(get_lat_lng, axis=1)
    out = df.copy()
    out["lat"] = lat_lng.apply(lambda x: to_float(x[0]))
    out["lng"] = lat_lng.apply(lambda x: to_float(x[1]))
    return out
