"""
Map reservation status codes to Croatian labels.
"""

# ReservationStatus enum values from Breeze DB
RESERVATION_STATUS_LABELS: dict[int, str] = {
    -2: "Nepoznato",
    -1: "Čekanje",
    0: "Na čekanju",
    1: "Potvrđeno",
    2: "Otkazano",
    3: "Odbijeno",
}


def reservation_status_label(status: int) -> str:
    """
    Map reservation status code to Croatian name.

    Args:
        status: Status code from Breeze DB (e.g. -1, 0, 1, 2, 3).

    Returns:
        Croatian label for the status, or "Nepoznato" for unknown codes.
    """
    return RESERVATION_STATUS_LABELS.get(status, "Nepoznato")
