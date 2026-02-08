"""
Compute distance between two lat/lng pairs using the Haversine formula.
Returns great-circle distance in kilometers.
"""

import math

EARTH_RADIUS_KM = 6371.0


def distance_km(
    p1: tuple[float, float],
    p2: tuple[float, float],
) -> float:
    """
    Compute great-circle distance between two points on Earth.
    Uses the Haversine formula.

    Args:
        p1: First point (lat, lng) in degrees.
        p2: Second point (lat, lng) in degrees.

    Returns:
        Distance in kilometers.
    """
    lat1, lng1 = p1
    lat2, lng2 = p2
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return EARTH_RADIUS_KM * c
