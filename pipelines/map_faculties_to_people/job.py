"""
Map faculties to people: update faculty column and add faculty_city from data/dist/faculties.csv.
Uses same normalization as faculties pipeline so raw names like "Fakultet za forenzičke znanosti bez pravne osobnosti (Split)"
map correctly to canonical "Fakultet za forenzičke znanosti (Split)" and get faculty_city=Split.
"""

from pathlib import Path

import pandas as pd

from pipelines.faculties.normalize import city_from_faculty_name, clean_faculty


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def main() -> None:
    root = _project_root()
    people_path = root / "data" / "dist" / "people.csv"
    faculties_path = root / "data" / "dist" / "faculties.csv"

    if not people_path.exists():
        raise FileNotFoundError(f"Input not found: {people_path}")
    if not faculties_path.exists():
        raise FileNotFoundError(f"Faculties not found: {faculties_path}")

    faculties_df = pd.read_csv(faculties_path)
    faculty_to_city = {
        row["faculty"]: (row["city"] if pd.notna(row["city"]) and str(row["city"]).strip() else "")
        for _, row in faculties_df.iterrows()
    }

    df = pd.read_csv(people_path)
    raw_faculty = df["faculty"].copy() if "faculty" in df.columns else pd.Series([""] * len(df))

    def _resolve(raw: str) -> tuple[str, str]:
        rs = str(raw).strip() if pd.notna(raw) else ""
        cleaned = clean_faculty(rs) if rs else None
        faculty = cleaned if cleaned else rs
        city = faculty_to_city.get(faculty, "") or "" if faculty else ""
        if not city and faculty:
            city = city_from_faculty_name(faculty)
        return faculty, str(city) if city else ""

    resolved = raw_faculty.apply(lambda x: _resolve(x) if pd.notna(x) else ("", ""))
    faculty_col = resolved.apply(lambda r: r[0])
    city_col = resolved.apply(lambda r: r[1])

    for col in ("faculty", "faculty_city"):
        if col in df.columns:
            df = df.drop(columns=[col])

    df["faculty"] = faculty_col.values
    df["faculty_city"] = city_col.fillna("").values

    df.to_csv(people_path, index=False)
    print(f"Updated: {people_path}")


if __name__ == "__main__":
    main()
