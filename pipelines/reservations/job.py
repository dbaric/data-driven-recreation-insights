"""
Reservations pipeline: data/source/data.db -> data/dist/reservations.csv

Excludes reservations for cancelled or deleted events.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import sqlite3

from pipelines.events.job import IGNORE_TITLES, load_events
from utils.distance import distance_km


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _load_reservations(db_path: Path):
    ignore_sql = ", ".join(f"'{t}'" for t in IGNORE_TITLES)
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"""
        SELECT u.id as userId, e.id as eventId, r.id as reservationId,
               r.status, r.attendedAt, r.createdAt, r.updatedAt, r.deletedAt
        FROM Reservation r
        JOIN "Option" o ON r.optionId = o.id
        JOIN Event e ON o.eventId = e.id
        JOIN User u ON r.userId = u.id
        WHERE r.deletedAt IS NULL
        AND e.deletedAt IS NULL
        AND e.cancelledAt IS NULL
        AND e.title NOT IN ({ignore_sql})
    """, conn)
    conn.close()
    return df


def _add_time_diff_columns(df: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """Add createdAt, attendedAt, cancelledAt differences from event startsAt (milliseconds)."""
    events_starts = events[["id", "startsAt"]].copy()
    events_starts["id"] = events_starts["id"].astype(str)
    df = df.merge(
        events_starts,
        left_on=df["eventId"].astype(str),
        right_on="id",
        how="left",
    )
    starts_ms = pd.to_numeric(df["startsAt"], errors="coerce")
    created_ms = pd.to_numeric(df["createdAt"], errors="coerce")
    attended_ms = pd.to_numeric(df["attendedAt"], errors="coerce")
    updated_ms = pd.to_numeric(df["updatedAt"], errors="coerce")

    df["createdAtMinusStartsAt"] = created_ms - starts_ms
    df["attendedAtMinusStartsAt"] = attended_ms - starts_ms

    cancelled_mask = df["status"] == 2
    df["cancelledAtMinusStartsAt"] = None
    df.loc[cancelled_mask, "cancelledAtMinusStartsAt"] = (
        updated_ms.loc[cancelled_mask] - starts_ms.loc[cancelled_mask]
    )

    df = df.drop(columns=["id", "startsAt"], errors="ignore")
    return df


def _load_people_expanded(root: Path) -> pd.DataFrame:
    """Load people.csv and expand comma-separated user_ids for merging."""
    path = root / "data" / "dist" / "people.csv"
    if not path.exists():
        return pd.DataFrame(columns=["user_id", "date_of_birth", "lat", "lng"])
    people = pd.read_csv(path, usecols=["user_id", "date_of_birth", "lat", "lng"])
    people["user_id"] = people["user_id"].astype(str).str.split(r",\s*")
    return people.explode("user_id", ignore_index=True)


def _add_age_and_distance(
    df: pd.DataFrame,
    people: pd.DataFrame,
    events: pd.DataFrame,
) -> pd.DataFrame:
    """Add ageAtReservation (years) and distanceKm (event to person residence)."""
    events_loc = events[["id", "lat", "lng"]].copy()
    events_loc["id"] = events_loc["id"].astype(str)
    df = df.merge(
        events_loc,
        left_on=df["eventId"].astype(str),
        right_on="id",
        how="left",
        suffixes=("", "_event"),
    )
    df = df.rename(columns={"lat": "event_lat", "lng": "event_lng"})
    df = df.drop(columns=["id"], errors="ignore")

    people_sub = people[["user_id", "date_of_birth", "lat", "lng"]].copy()
    people_sub = people_sub.rename(columns={"lat": "person_lat", "lng": "person_lng"})
    df = df.merge(
        people_sub,
        left_on=df["userId"].astype(str),
        right_on="user_id",
        how="left",
    )
    df = df.drop(columns=["user_id"], errors="ignore")

    created_ms = pd.to_numeric(df["createdAt"], errors="coerce")
    dob_dt = pd.to_datetime(df["date_of_birth"], format="%Y-%m-%d", errors="coerce")
    ms_per_year = 365.25 * 24 * 3600 * 1000
    df["ageAtReservation"] = np.nan
    valid_dob = dob_dt.notna()
    df.loc[valid_dob, "ageAtReservation"] = (
        created_ms.loc[valid_dob] - dob_dt.loc[valid_dob].astype("int64") / 1e6
    ) / ms_per_year

    def _dist(row):
        elat, elng = row.get("event_lat"), row.get("event_lng")
        plat, plng = row.get("person_lat"), row.get("person_lng")
        if pd.isna(elat) or pd.isna(elng) or pd.isna(plat) or pd.isna(plng):
            return np.nan
        return distance_km((float(elat), float(elng)), (float(plat), float(plng)))

    df["distanceKm"] = df.apply(_dist, axis=1)
    df = df.drop(
        columns=["event_lat", "event_lng", "person_lat", "person_lng", "date_of_birth"],
        errors="ignore",
    )
    return df


def load_reservations(db_path: Path | None = None, *, skip_geocode: bool = True):
    """Load and process reservations from database. Returns processed DataFrame."""
    root = _project_root()
    path = Path(db_path) if db_path else root / "data" / "source" / "data.db"
    df = _load_reservations(path)

    events = load_events(path, skip_geocode=skip_geocode)
    valid_event_ids = set(events["id"].astype(str))
    df = df[df["eventId"].astype(str).isin(valid_event_ids)]
    df = _add_time_diff_columns(df, events)

    people = _load_people_expanded(root)
    df = _add_age_and_distance(df, people, events)
    return df


def main() -> None:
    root = _project_root()
    (root / "data" / "dist").mkdir(parents=True, exist_ok=True)

    db_path = root / "data" / "source" / "data.db"
    output_path = root / "data" / "dist" / "reservations.csv"

    if not db_path.exists():
        raise FileNotFoundError(f"Input database not found: {db_path}")

    df = load_reservations(db_path)
    df.to_csv(output_path, index=False)
    print(f"Output: {output_path} ({len(df)} reservations)")


if __name__ == "__main__":
    main()
