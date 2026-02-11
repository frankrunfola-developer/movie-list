#!/usr/bin/env python3
"""
enrich_movies_omdb_csv.py

Enrich a movie CSV using OMDb.
- Input CSV must include: Title (Year optional)
- Output fills: Year, Genre, imdbRating, Actors (Main), BoxOffice
- Prints one line per movie as it is queried (or served from cache)

Env:
  OMDB_API_KEY=your_key

Run:
  python enrich_movies_omdb_csv.py input.csv enriched.csv --xlsx enriched.xlsx
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from typing import Any, Dict, Optional

import pandas as pd
import requests

OMDB_URL = "https://www.omdbapi.com/"


def clean_text(x: Any) -> str:
    """Safe string conversion; normalizes smart quotes; strips."""
    if x is None or pd.isna(x):
        return ""
    s = str(x)
    s = (
        s.replace("’", "'")
        .replace("‘", "'")
        .replace("“", '"')
        .replace("”", '"')
        .strip()
    )
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_year(x: Any) -> str:
    """Return only a clean 4-digit year, else '' (prevents y=<NA> and other junk)."""
    s = clean_text(x)
    return s if (len(s) == 4 and s.isdigit()) else ""


def put_if_value(df: pd.DataFrame, i: int, col: str, new_val: Any) -> None:
    """Write new_val to df[i,col] only if new_val is a non-empty string after cleaning."""
    v = clean_text(new_val)
    if v != "":
        df.loc[i, col] = v


def load_cache(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(path: str, cache: Dict[str, Any]) -> None:
    if not path:
        return
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def make_key(title: str, year: str) -> str:
    return f"{title.strip().lower()}||{year.strip()}"


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = ["Row", "Title", "Year", "Genre", "imdbRating", "Actors (Main)", "BoxOffice"]
    for c in required:
        if c not in df.columns:
            df[c] = ""
    extras = [c for c in df.columns if c not in required]
    return df[required + extras]


def omdb_get(params: Dict[str, str], api_key: str, retries: int) -> Dict[str, Any]:
    params = dict(params)
    params["apikey"] = api_key

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(OMDB_URL, params=params, timeout=30)

            if r.status_code != 200:
                snippet = (r.text or "")[:200].replace("\n", " ")
                raise RuntimeError(f"HTTP {r.status_code}: {snippet}")

            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.8 * (2 ** (attempt - 1)))

    raise RuntimeError(f"OMDb request failed after {retries} retries: {last_err}") from last_err


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("input_csv", help="Input CSV (must include Title)")
    ap.add_argument("output_csv", help="Output enriched CSV")
    ap.add_argument("--xlsx", default="", help="Optional output XLSX path")
    ap.add_argument("--cache", default="omdb_cache.json", help="Cache JSON (default: omdb_cache.json)")
    ap.add_argument("--sleep", type=float, default=0.5, help="Delay between API calls (default: 0.5)")
    ap.add_argument("--retries", type=int, default=5, help="Retries per request (default: 5)")
    ap.add_argument("--print-json", action="store_true", help="Also print raw OMDb JSON per title (VERY noisy)")
    args = ap.parse_args()

    api_key = os.getenv("OMDB_API_KEY", "").strip()
    if not api_key:
        print("ERROR: OMDB_API_KEY is not set.", file=sys.stderr)
        return 2

    df = pd.read_csv(args.input_csv)
    df = ensure_columns(df)

    # Force target columns to TEXT so pandas never throws dtype errors
    text_cols = ["Title", "Year", "Genre", "imdbRating", "Actors (Main)", "BoxOffice"]
    for c in text_cols:
        df[c] = df[c].astype("string")

    cache = load_cache(args.cache)

    total = len(df)
    cache_hits = 0
    api_calls = 0

    for i in range(total):
        title = clean_text(df.loc[i, "Title"])
        if not title:
            continue

        year = normalize_year(df.loc[i, "Year"])
        key = make_key(title, year)

        params: Dict[str, str] = {"t": title, "plot": "short", "r": "json"}
        if year:
            params["y"] = year  # only valid 4-digit years get here

        try:
            if key in cache:
                data = cache[key]
                cache_hits += 1
                src = "CACHE"
            else:
                data = omdb_get(params, api_key, retries=args.retries)
                cache[key] = data
                save_cache(args.cache, cache)
                api_calls += 1
                src = "API"
                time.sleep(args.sleep)

            # Print one line per movie as requested
            if isinstance(data, dict) and data.get("Response") == "True":
                found_title = clean_text(data.get("Title", ""))
                found_year = clean_text(data.get("Year", ""))
                rating = clean_text(data.get("imdbRating", ""))
                print(f"[{i+1}/{total}] {src} OK | {title} ({year or '----'}) -> {found_title} ({found_year}) | imdb={rating}")
            else:
                err = clean_text((data or {}).get("Error", "Unknown"))
                print(f"[{i+1}/{total}] {src} NOT FOUND | {title} ({year or '----'}) | {err}")

            if args.print_json:
                print(f"JSON: {data}\n")

        except Exception as e:
            # Print and keep going
            print(f"[{i+1}/{total}] ERROR | {title} ({year or '----'}) | {e}")
            df.loc[i, "imdbRating"] = f"ERROR: {e}"
            continue

        # Write columns if response is valid
        if isinstance(data, dict) and data.get("Response") == "True":
            if not year:
                put_if_value(df, i, "Year", data.get("Year", ""))
            put_if_value(df, i, "Genre", data.get("Genre", ""))
            put_if_value(df, i, "imdbRating", data.get("imdbRating", ""))
            put_if_value(df, i, "Actors (Main)", data.get("Actors", ""))
            put_if_value(df, i, "BoxOffice", data.get("BoxOffice", ""))
        else:
            # Tag not found (keeps a record) — avoid boolean ops on pd.NA
            err = clean_text((data or {}).get("Error", "Unknown"))
            current = df.loc[i, "imdbRating"]
            if pd.isna(current) or clean_text(current) == "":
                df.loc[i, "imdbRating"] = f"NOT FOUND: {err}"

        # Keep Row sequential
        df.loc[i, "Row"] = i + 1

    df.to_csv(args.output_csv, index=False)
    if args.xlsx:
        df.to_excel(args.xlsx, index=False, sheet_name="Movies")

    print("\nDONE")
    print(f"Output CSV: {args.output_csv}")
    if args.xlsx:
        print(f"Output XLSX: {args.xlsx}")
    print(f"Cache hits: {cache_hits}")
    print(f"API calls:   {api_calls}")
    print(f"Cache file:  {args.cache}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
