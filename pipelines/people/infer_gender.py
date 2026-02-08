"""
Infer gender from Croatian first names.
Uses gender-guesser (offline) with country='croatia' and explicit unisex list.
"""

import pandas as pd
import gender_guesser.detector as gender

# Unisex imena – samo ona koja su ISTINSKI i muška i ženska u HR
HR_UNISEX = frozenset({
    "Saša", "Sasha", "Sasa", "Đani", "Dani", "Borna",
    "Kim", "Alex", "Sam",
})

# Ženska imena – sigurno žensko kad inferiramo (preskače se ako postoji izvorni gender)
HR_FEMALE = frozenset({"neri", "iris", "natali", "stefani"})

_detector = None


def _get_detector():
    global _detector
    if _detector is None:
        _detector = gender.Detector(case_sensitive=False)
    return _detector


def infer_gender(first_name: str, existing_gender=None) -> str:
    """
    Vraća SAMO: male, female, unisex, unknown.

    Kad je gender="Ž"/"ž" → female. Kad je "M"/"m" → male.
    """
    if pd.notna(existing_gender) and str(existing_gender).strip():
        val = str(existing_gender).strip().lower()
        if val in ("male", "m", "muško"):
            return "male"
        if val in ("female", "f", "ž", "žensko"):
            return "female"

    name = str(first_name).strip() if pd.notna(first_name) else ""
    if not name:
        return "unknown"

    normalized = name.strip()
    if normalized in HR_UNISEX:
        return "unisex"
    if normalized.lower() in HR_FEMALE:
        return "female"

    d = _get_detector()
    result = d.get_gender(normalized, "croatia")

    mapping = {
        "male": "male",
        "mostly_male": "male",
        "female": "female",
        "mostly_female": "female",
    }
    if result in mapping:
        return mapping[result]

    # Fallback: gender-guesser vraća "andy"/"unknown" za mnoga hrvatska imena.
    # U hrvatskom: -a obično žensko, sve ostalo obično muško.
    if result in ("andy", "unknown"):
        last = normalized[-1].lower() if len(normalized) > 1 else ""
        if last == "a":
            return "female"
        return "male"
    return "unknown"


def _normalize_gender(val: str) -> str:
    """Osigurava da je output samo male/female/unisex/unknown."""
    if pd.isna(val) or not str(val).strip():
        return "unknown"
    v = str(val).strip().lower()
    if v in ("ž", "female", "f", "žensko"):
        return "female"
    if v in ("m", "male", "muško"):
        return "male"
    if v in ("unisex", "unknown"):
        return v
    return "unknown"


def add_gender_inferred(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dodaje stupac gender_inferred u dataframe.
    Koristi firstName i postojeći gender (ako postoji).
    Output je UVIJEK male/female/unisex/unknown.
    """
    df = df.copy()
    df["gender_inferred"] = df.apply(
        lambda r: _normalize_gender(infer_gender(r.get("firstName"), r.get("gender"))),
        axis=1,
    )
    return df
