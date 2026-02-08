"""
Shared faculty normalization used by faculties pipeline and map_faculties_to_people.
"""

import re

# Only treat parenthetical suffix as city if it's a known Croatian city.
# Prevents "Fakultet elektrotehnike (strojarstva i brodogradnje)" -> city="strojarstva i brodogradnje"
KNOWN_CITIES = frozenset({
    "Čakovec", "Dubrovnik", "Koprivnica", "Osijek", "Pula", "Rijeka",
    "Slavonski Brod", "Split", "Šibenik", "Vukovar", "Velika Gorica",
    "Zadar", "Zagreb",
})

PRELATAK_PATTERN = re.compile(r"^Prelazak\s+.+\s+u\s+(.+)$", re.IGNORECASE)
ODJEL_TO_FAKULTET = {
    "zdravstvenih studija": "Fakultet zdravstvenih znanosti",
    "za forenzičke znanosti": "Fakultet za forenzičke znanosti",
    "za forenzične znanosti": "Fakultet za forenzičke znanosti",
    "za stručne studije": "Sveučilišni odjel za stručne studije",
    "za studije mora": "Sveučilišni odjel za studije mora",
}
# Malformed: "(strojarstva i brodogradnje)" is sub-discipline, not city. Correct form is FESB Split.
FAKULTET_ELEKTRO_STROJARSTVO = re.compile(
    r"Fakultet elektrotehnike\s*\(strojarstva i brodogradnje\)",
    re.IGNORECASE,
)
# Prirodoslovno-matematički = Prirodoslovno - matematički (isti fakultet)
PRIRODOSLOVNO_MATEMATICKI = re.compile(
    r"Prirodoslovno\s*-\s*matematički fakultet",
    re.IGNORECASE,
)
# Univerzalna imena bez grada -> kanonski oblik (jedino sveučilište te vrste u Hrvatskoj)
AMBIGUOUS_TO_CANONICAL = {
    "aspira": "Veleučilište Aspira (Split)",
    "pomorski fakultet": "Pomorski fakultet u Splitu (Split)",
    "umjetnička akademija": "Umjetnička akademija (Split)",
    "sveučilišni odjel za stručne studije": "Sveučilišni odjel za stručne studije (Split)",
    "prirodoslovno-matematički fakultet": "Prirodoslovno - matematički fakultet (Split)",
}
SKIP_VALUES = {"", "Nema podataka.", "nan"}
BEZ_PRAVNE_OSOBNOSTI = re.compile(r"\s+bez pravne osobnosti\s*", re.IGNORECASE)


def _strip_wrapping_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        return s[1:-1]
    return s


def _normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip())


def _apply_prelazak(s: str) -> str | None:
    m = PRELATAK_PATTERN.match(s)
    return m.group(1).strip() if m else None


def extract_location_suffix(s: str) -> tuple[str, str]:
    """Return (base_without_location, location_or_empty). Only treat (X) as city if X is in KNOWN_CITIES."""
    m = re.search(r"\s*\(([^)]+)\)\s*$", s)
    if m:
        loc = m.group(1).strip()
        if loc in KNOWN_CITIES:
            return s[: m.start()].strip(), f" ({loc})"
        return s, ""
    m2 = re.search(r",\s*([^,]+)\s*$", s)
    if m2:
        loc = m2.group(1).strip()
        if loc in KNOWN_CITIES:
            return s[: m2.start()].strip(), f" ({loc})"
        return s, ""
    return s, ""


def _normalize_location_format(s: str) -> str:
    """Convert 'X, Split' to 'X (Split)' when Split is a known city.
    Ako grad nije naveden, odnosi se na Split."""
    base, suffix = extract_location_suffix(s)
    return base + (suffix if suffix else " (Split)")


def _apply_odjel_mapping(s: str) -> str | None:
    base, loc_suffix = extract_location_suffix(s)
    lower = base.lower()
    for key, canonical in ODJEL_TO_FAKULTET.items():
        if key in lower:
            return canonical + loc_suffix
    return None


def clean_faculty(raw: str) -> str | None:
    """Normalize raw faculty name to canonical form matching faculties.csv."""
    s = _normalize_whitespace(raw)
    if not s or s in SKIP_VALUES:
        return None

    lower = s.lower()
    for key, canonical in AMBIGUOUS_TO_CANONICAL.items():
        if lower == key or lower == key.strip():
            return canonical

    prelazak = _apply_prelazak(s)
    if prelazak is not None:
        s = prelazak

    odjel = _apply_odjel_mapping(s)
    if odjel is not None:
        s = odjel

    s = FAKULTET_ELEKTRO_STROJARSTVO.sub("Fakultet elektrotehnike, strojarstva i brodogradnje (Split)", s)
    s = PRIRODOSLOVNO_MATEMATICKI.sub("Prirodoslovno - matematički fakultet", s)
    s = _normalize_location_format(s)
    s = BEZ_PRAVNE_OSOBNOSTI.sub(" ", s)
    s = _normalize_whitespace(s)
    s = _strip_wrapping_quotes(s)

    return s if s else None


def city_from_faculty_name(f: str) -> str:
    """Extract city from faculty name suffix, e.g. ' (Split)' -> 'Split'."""
    _, suffix = extract_location_suffix(f)
    return suffix[2:-1] if suffix.startswith(" (") else ""
