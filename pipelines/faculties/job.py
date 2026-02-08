"""
Faculties pipeline: extract unique faculties from people.csv, cleanup, save to data/dist/faculties.csv
"""

import sys
from pathlib import Path

import pandas as pd

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from pipelines.faculties.normalize import city_from_faculty_name, clean_faculty


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def main() -> None:
    root = _project_root()
    people_path = root / "data" / "dist" / "people.csv"
    output_path = root / "data" / "dist" / "faculties.csv"

    if not people_path.exists():
        raise FileNotFoundError(f"Input not found: {people_path}")

    df = pd.read_csv(people_path)
    raw_faculties = df["faculty"].dropna().astype(str)
    unique_raw = raw_faculties.unique().tolist()

    cleaned = set()
    for raw in unique_raw:
        c = clean_faculty(raw)
        if c:
            cleaned.add(c)

    faculties_sorted = sorted(cleaned)

    rows = [
        {"id": i, "faculty": f, "city": city_from_faculty_name(f)}
        for i, f in enumerate(faculties_sorted, start=1)
    ]
    out_df = pd.DataFrame(rows)
    out_df.to_csv(output_path, index=False)
    print(f"Output: {output_path} ({len(faculties_sorted)} faculties)")


if __name__ == "__main__":
    main()
