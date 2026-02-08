"""
People pipeline: data/source/data.db -> data/dist/people.csv
"""

import os
import re
import sqlite3
import sys
import unicodedata
from pathlib import Path

import pandas as pd

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from pipelines.people.geocode_residence import add_lat_lng_to_df
from pipelines.people.infer_gender import add_gender_inferred
from pipelines.people.parse_dirty_socks import (
    extract_country_code_and_address,
    parse_dirty_socks,
    to_iso_date,
)

SEP = " | "

DROP_COLUMNS = [
    "image",
    "organizationId",
    "greenFlaggedAt",
    "redFlaggedAt",
    "isConsent",
    "password",
    "repeatPassword",
    "domicile",
]

OUTPUT_COLUMNS = [
    "createdAt",
    "updatedAt",
    "gender",
    "faculty",
    "user_id",
    "date_of_birth",
    "lat",
    "lng",
]


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


IGNORE_PEOPLE: set[tuple[str, str]] = {
    ("Dražen", "Barić"),
    ("Magdalena", "Ramljak"),
    ("Marija", "Ćubić"),
    ("Nikolina", "Carević"),
}


def _ensure_dirs() -> None:
    root = _project_root()
    (root / "data" / "dist").mkdir(parents=True, exist_ok=True)
    (root / "data" / "cache").mkdir(parents=True, exist_ok=True)


def _load_users(db_path: Path) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query('SELECT * FROM "User"', conn)
    conn.close()
    return df


def _select_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=[c for c in DROP_COLUMNS if c in df.columns], errors="ignore")


def _parse_and_merge_dirty_socks(df: pd.DataFrame) -> pd.DataFrame:
    parsed = df["dirtySocks"].apply(parse_dirty_socks)
    socks = pd.DataFrame(parsed.tolist(), index=df.index)

    socks["dirtySocks_datum_rodjenja"] = socks["dirtySocks_datum_rodjenja"].apply(to_iso_date)
    socks["dirtySocks_telefon"] = socks["dirtySocks_telefon"].apply(
        lambda v: v.replace(" ", "") if pd.notna(v) and v else v
    )

    preb_vals = socks["dirtySocks_prebivaliste"].apply(
        lambda v: extract_country_code_and_address(v) if pd.notna(v) else (None, None)
    )
    socks["residence"] = preb_vals.apply(lambda x: x[0])
    socks["country_code"] = preb_vals.apply(lambda x: x[1])

    users_clean = df.drop(columns=["dirtySocks"], errors="ignore").copy()
    users_clean["user_id"] = users_clean["id"]

    merged = users_clean.merge(
        socks[
            [
                "residence",
                "country_code",
                "dirtySocks_drzavljanstvo",
                "dirtySocks_oib",
                "dirtySocks_jmbag",
                "dirtySocks_esi",
                "dirtySocks_datum_rodjenja",
                "dirtySocks_telefon",
            ]
        ].rename(
            columns={
                "dirtySocks_drzavljanstvo": "citizenship",
                "dirtySocks_oib": "oib",
                "dirtySocks_jmbag": "jmbag",
                "dirtySocks_esi": "esi",
                "dirtySocks_datum_rodjenja": "date_of_birth",
                "dirtySocks_telefon": "phone",
            }
        ),
        left_index=True,
        right_index=True,
        how="left",
    )

    merged["oib"] = merged.apply(_merge_oib_row, axis=1)
    merged = merged.drop(columns=["id", "taxNumber", "cardId"], errors="ignore")

    merged["date_of_birth"] = merged.apply(_merge_date_row, axis=1)
    merged = merged.drop(columns=["dateOfBirth"], errors="ignore")

    merged["phone"] = merged.apply(_merge_phone_row, axis=1)
    merged = merged.drop(columns=["phone_x", "phone_y"], errors="ignore")

    merged = merged.drop(
        columns=["organizationId", "greenFlaggedAt", "redFlaggedAt", "isConsent", "password", "repeatPassword"],
        errors="ignore",
    )

    return merged


def _merge_oib_row(row) -> str | None:
    for key in ("oib", "taxNumber", "cardId"):
        v = row.get(key)
        if pd.notna(v) and str(v).strip() and str(v).strip() != "nan":
            s = str(v).strip()
            if isinstance(v, float) and v == int(v):
                return str(int(v))
            return s
    return None


def _merge_date_row(row) -> str | None:
    dob = row.get("date_of_birth")
    if pd.notna(dob) and str(dob).strip() and str(dob).strip() != "nan":
        return str(dob).strip()
    ts = row.get("dateOfBirth")
    if pd.notna(ts):
        dt = pd.to_datetime(ts, unit="ms", errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%Y-%m-%d")
    return None


def _merge_phone_row(row) -> str | None:
    for key in ("phone", "phone_x", "phone_y"):
        v = row.get(key)
        if pd.notna(v):
            if isinstance(v, (int, float)) and v == int(v):
                return str(int(v))
            s = str(v).strip()
            if s and s.lower() != "nan":
                return s
    return None


def _merge_oib_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    oib_dup = df[df["oib"].notna()].groupby("oib").filter(lambda g: len(g) > 1)
    if len(oib_dup) == 0:
        return df
    oib_single = df[~df.index.isin(oib_dup.index)]
    merged_rows = []
    for oib, group in oib_dup.groupby("oib"):
        row = group.iloc[0].to_dict()
        row["createdAt"] = group["createdAt"].min()
        row["updatedAt"] = group["updatedAt"].min()
        phones = (
            group["phone"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("nan", "")
            .replace("<NA>", "")
        )
        phones = phones[phones != ""].unique().tolist()
        row["phone"] = SEP.join(phones) if len(phones) > 1 else (phones[0] if phones else None)
        emails = (
            group["email"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("nan", "")
            .replace("<NA>", "")
        )
        emails = emails[emails != ""].unique().tolist()
        row["email"] = SEP.join(emails) if len(emails) > 1 else (emails[0] if emails else None)
        dobs = group["date_of_birth"].dropna()
        dobs = pd.to_datetime(dobs, errors="coerce").dropna()
        row["date_of_birth"] = (
            dobs.iloc[0].strftime("%Y-%m-%d")
            if len(dobs) > 0
            else group["date_of_birth"].iloc[0]
        )
        row["user_id"] = ", ".join(group["user_id"].dropna().astype(str).unique())
        row["faculty"] = group["faculty"].dropna().iloc[0] if group["faculty"].notna().any() else None
        countries = (
            group["country"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("nan", "")
            .replace("<NA>", "")
        )
        countries = countries[countries != ""].unique().tolist()
        row["country"] = ", ".join(countries) if len(countries) > 1 else (countries[0] if countries else None)
        merged_rows.append(row)
    return pd.concat([oib_single, pd.DataFrame(merged_rows)], ignore_index=True)


def _strip_hr(s) -> str:
    if pd.isna(s) or not str(s).strip():
        return ""
    t = unicodedata.normalize("NFD", str(s).strip().lower())
    return "".join(c for c in t if unicodedata.category(c) != "Mn")


def _merge_dofb_lastname_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["_ln"] = out["lastName"].apply(_strip_hr)
    out["_dofb"] = out["date_of_birth"].fillna("")
    dup = (
        out[(out["_dofb"] != "") & (out["_ln"] != "")]
        .groupby(["_dofb", "_ln"])
        .filter(lambda g: len(g) > 1)
    )
    if len(dup) == 0:
        return out.drop(columns=["_ln", "_dofb"], errors="ignore")

    merge_indices = set()
    for (dofb, ln), grp in dup.groupby(["_dofb", "_ln"]):
        oibs = (
            grp["oib"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.replace("nan", "")
            .str.replace("<NA>", "")
        )
        oibs = oibs[oibs != ""].unique()
        if len(oibs) != 1:
            continue
        merge_indices.update(grp.index.tolist())

    if not merge_indices:
        return out.drop(columns=["_ln", "_dofb"], errors="ignore")

    non_merge = out[~out.index.isin(merge_indices)].copy()
    merged_rows = []
    for (dofb, ln), grp in dup.groupby(["_dofb", "_ln"]):
        oibs = (
            grp["oib"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.replace("nan", "")
            .str.replace("<NA>", "")
        )
        oibs = oibs[oibs != ""].unique()
        if len(oibs) != 1:
            continue
        grp_sorted = grp.sort_values("createdAt")
        oldest = grp_sorted.iloc[0]
        row = {}
        row["createdAt"] = grp["createdAt"].min()
        row["updatedAt"] = grp["updatedAt"].min()
        row["faculty"] = oldest["faculty"]
        cols_exclude = {"_ln", "_dofb", "createdAt", "updatedAt", "faculty"}
        for col in grp.columns:
            if col in cols_exclude:
                continue
            vals = (
                grp[col]
                .dropna()
                .astype(str)
                .str.strip()
                .str.replace("nan", "")
                .str.replace("<NA>", "")
            )
            vals = vals[vals != ""].unique().tolist()
            if len(vals) == 0:
                row[col] = None
            elif len(vals) == 1:
                row[col] = grp[col].dropna().iloc[0]
            else:
                row[col] = ", ".join(vals) if col == "user_id" else SEP.join(vals)
        merged_rows.append(row)

    result = pd.concat(
        [non_merge.drop(columns=["_ln", "_dofb"], errors="ignore"), pd.DataFrame(merged_rows)],
        ignore_index=True,
    )
    return result


def main() -> None:
    root = _project_root()
    _ensure_dirs()

    db_path = root / "data" / "source" / "data.db"
    output_path = root / "data" / "dist" / "people.csv"

    if not db_path.exists():
        raise FileNotFoundError(f"Input database not found: {db_path}")

    df = _load_users(db_path)

    ignored = df.apply(
        lambda row: (
            str(row.get("firstName") or "").strip(),
            str(row.get("lastName") or "").strip(),
        )
        in IGNORE_PEOPLE,
        axis=1,
    )
    df = df[~ignored].copy()

    limit = int(os.environ.get("PEOPLE_PIPELINE_LIMIT", 0)) or None
    if limit:
        df = df.head(limit)
    df = _select_columns(df)

    df = _parse_and_merge_dirty_socks(df)
    skip_geocode = os.environ.get("PEOPLE_PIPELINE_SKIP_GEOCODE", "").lower() in ("1", "true", "yes")
    df = add_lat_lng_to_df(df, skip_api=skip_geocode)
    df = add_gender_inferred(df)

    df = _merge_oib_duplicates(df)
    df = _merge_dofb_lastname_duplicates(df)

    df = df.drop(columns=["i", "phone"], errors="ignore")
    df = df.drop(columns=["gender"], errors="ignore").rename(columns={"gender_inferred": "gender"})
    cols = [c for c in OUTPUT_COLUMNS if c in df.columns]
    df[cols].to_csv(output_path, index=False)
    print(f"Output: {output_path}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Limit users for testing")
    parser.add_argument("--skip-geocode", action="store_true", help="Only use cache, skip Nominatim API")
    args = parser.parse_args()
    if args.limit:
        os.environ["PEOPLE_PIPELINE_LIMIT"] = str(args.limit)
    if args.skip_geocode:
        os.environ["PEOPLE_PIPELINE_SKIP_GEOCODE"] = "1"
    main()
