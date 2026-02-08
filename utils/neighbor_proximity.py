"""
Compute geographic proximity (susjedstvo) between attendees at events.
Uses residence lat/lng to detect "neighbors" — attendees who live within radius_km of each other.
"""

import pandas as pd

from utils.distance import distance_km


def had_neighbor_per_attendance(
    df_attended: pd.DataFrame,
    people_lat_lng: pd.DataFrame,
    event_col: str = "eventId",
    user_col: str = "userId",
    radius_km: float = 10.0,
) -> pd.DataFrame:
    """
    For each (eventId, userId) in attended rows, compute had_neighbor:
    at least one other attendee on the same event lives within radius_km.

    Args:
        df_attended: DataFrame with event_col, user_col (only attended rows).
        people_lat_lng: DataFrame with user_col, 'lat', 'lng' (residence coordinates).
        radius_km: Max distance in km to count as neighbor.

    Returns:
        DataFrame with event_col, user_col, 'had_neighbor' (bool).
    """
    coords = people_lat_lng.dropna(subset=["lat", "lng"]).set_index(user_col)[["lat", "lng"]]
    results = []

    for eid, grp in df_attended.groupby(event_col):
        attendees = grp[user_col].unique().tolist()
        for uid in attendees:
            if uid not in coords.index:
                results.append({event_col: eid, user_col: uid, "had_neighbor": False})
                continue
            my = coords.loc[uid]
            my_lat, my_lng = float(my["lat"]), float(my["lng"])
            had = False
            for oid in attendees:
                if oid == uid:
                    continue
                if oid not in coords.index:
                    continue
                other = coords.loc[oid]
                d = distance_km((my_lat, my_lng), (float(other["lat"]), float(other["lng"])))
                if d <= radius_km:
                    had = True
                    break
            results.append({event_col: eid, user_col: uid, "had_neighbor": had})

    return pd.DataFrame(results)
