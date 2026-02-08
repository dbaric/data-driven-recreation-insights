"""
Events pipeline: data/source/data.db -> data/dist/events.csv
"""

import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
import sqlite3

from utils.geocode import add_lat_lng_for_column

IGNORE_TITLES = {"UNISPORT HEALTH DAY", "Svakodnevno testiranje1"}

TITLE_FIX = {
    "UNISPORT Scuba Diving school": "UNISPORT Scuba Diving School",
    "Unisport Scuba diving school": "UNISPORT Scuba Diving School",
    "UNISPORT S cuba Diving School": "UNISPORT Scuba Diving School",
    "Unisport S cuba Diving School": "UNISPORT Scuba Diving School",
    "Unisport S cuba Diving school": "UNISPORT Scuba Diving School",
}

EVENT_IS_WATER_SPORT = {"Swimming", "UNISPORT Scuba Diving School", "ROWfit"}
EVENT_IS_PAIRED = {"SALSA/BACHATA"}
EVENT_IS_TEAM = {"American football", "Futsal studenti", "Futsal studentice", "Košarka studenti/ce", "Lacrosse"}
EVENT_IS_INDIVIDUAL = {
    "Functional training", "HRVANJE", "JIU JITSU", "JUDO", "Kickboxing",
    "ROWfit", "Run student run", "Sport climbing", "Swimming",
    "UNISPORT Scuba Diving School", "UniFIT",
}
EVENT_IS_CARDIO = {"Swimming", "ROWfit", "Run student run"}
EVENT_IS_STRENGTH = {"Functional training", "UniFIT"}
EVENT_IS_BALL_SPORT = {"American football", "Futsal studenti", "Futsal studentice", "Košarka studenti/ce", "Lacrosse"}
EVENT_IS_COMBAT_SPORT = {"HRVANJE", "JUDO", "JIU JITSU", "Kickboxing"}
EVENT_IS_CONTACT_SPORT = {
    "American football", "Futsal studenti", "Futsal studentice", "Košarka studenti/ce", "Lacrosse",
    "HRVANJE", "JUDO", "JIU JITSU", "Kickboxing",
}
EVENT_IS_HIGH_INTENSITY = {"Kickboxing", "Functional training", "Sport climbing", "HRVANJE", "JUDO", "JIU JITSU"}


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _load_events(db_path: Path) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT e.id, e.title, e.location, e.startsAt, e.endsAt, e.cancelledAt,
               COALESCE(SUM(o.totalUnits), 0) as totalUnits
        FROM Event e
        LEFT JOIN "Option" o ON e.id = o.eventId AND o.deletedAt IS NULL
        GROUP BY e.id, e.title, e.location, e.startsAt, e.endsAt, e.cancelledAt
    """, conn)
    conn.close()
    return df


def _extract_group(title: str) -> tuple[str | None, str]:
    match = re.search(r" (group \d+)$", title, re.IGNORECASE)
    if match:
        return match.group(1), title[: match.start()].strip()
    return None, title


def _add_indoor_from_location(df: pd.DataFrame) -> pd.DataFrame:
    loc = df["location"].fillna("").str.lower()
    indoor_keywords = r"dvorana|bazen|bazeni|teretana|klub|škola"
    outdoor_keywords = r"teren|žnjan"
    df["isIndoor"] = np.where(
        loc.str.contains(outdoor_keywords, regex=True),
        False,
        np.where(loc.str.contains(indoor_keywords, regex=True), True, True),
    )
    df.loc[df["title"] == "ROWfit", "isIndoor"] = True
    return df


def _add_feature_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["isWaterSport"] = df["title"].isin(EVENT_IS_WATER_SPORT)
    df["isPaired"] = df["title"].isin(EVENT_IS_PAIRED)
    df["isTeamSport"] = df["title"].isin(EVENT_IS_TEAM)
    df["isIndividual"] = df["title"].isin(EVENT_IS_INDIVIDUAL)
    df["isCardio"] = df["title"].isin(EVENT_IS_CARDIO)
    df["isStrength"] = df["title"].isin(EVENT_IS_STRENGTH)
    df["isBallSport"] = df["title"].isin(EVENT_IS_BALL_SPORT)
    df["isCombatSport"] = df["title"].isin(EVENT_IS_COMBAT_SPORT)
    df["isContactSport"] = df["title"].isin(EVENT_IS_CONTACT_SPORT)
    df["isHighIntensity"] = df["title"].isin(EVENT_IS_HIGH_INTENSITY)
    df["isInDormitory"] = df["location"].fillna("").str.contains(
        r"Dom|Kampus|studentski dom", case=False, regex=True
    )
    return df


def process_events(df: pd.DataFrame, *, skip_geocode: bool = False) -> pd.DataFrame:
    df = df[~df["title"].isin(IGNORE_TITLES)].copy()

    result = df["title"].apply(_extract_group)
    df["group"] = result.apply(lambda x: x[0])
    df["title"] = result.apply(lambda x: x[1])

    df["title"] = df["title"].str.replace(r" -.*$", "", regex=True).str.strip()
    df["title"] = df["title"].replace(TITLE_FIX)

    df = _add_indoor_from_location(df)
    df = _add_feature_columns(df)

    df = add_lat_lng_for_column(df, "location", "HR", skip_api=skip_geocode, use_event_fallbacks=True)
    df = df.drop(columns=["location"], errors="ignore")

    return df


def load_events(db_path: Path | None = None, *, skip_geocode: bool = False) -> pd.DataFrame:
    """Load and process events from database. Returns processed DataFrame."""
    path = Path(db_path) if db_path else _project_root() / "data" / "source" / "data.db"
    raw = _load_events(path)
    return process_events(raw, skip_geocode=skip_geocode)


def main() -> None:
    root = _project_root()
    (root / "data" / "dist").mkdir(parents=True, exist_ok=True)

    db_path = root / "data" / "source" / "data.db"
    output_path = root / "data" / "dist" / "events.csv"

    if not db_path.exists():
        raise FileNotFoundError(f"Input database not found: {db_path}")

    skip_geocode = os.environ.get("EVENTS_PIPELINE_SKIP_GEOCODE", "").lower() in ("1", "true", "yes")
    df = load_events(db_path, skip_geocode=skip_geocode)
    df.to_csv(output_path, index=False)
    print(f"Output: {output_path} ({len(df)} events)")


if __name__ == "__main__":
    main()
