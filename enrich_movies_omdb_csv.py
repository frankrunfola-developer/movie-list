#!/usr/bin/env python3
"""
Enrich a movie CSV using the OMDb API.

Reads a CSV with at least a `Title` column (optionally `Year`),
then fills: Year, Genre, imdbRating, Actors (Main), BoxOffice.

See README.md for setup + commands.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, Optional

import pandas as pd
import requests

OMDB_URL = "https://www.omdbapi.com/"


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
    return f"{(title or '').strip().lower()}||{(year or '').strip()}"


def omdb_get(params: Dict[str, str], api_key: str, retries: int) -> Dict[str, Any]:
    params = dict(params)
    params["apikey"] = api_key

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(OMDB_URL, params=params, timeout=30)
            print(f"  r ={r.json}\n")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.8 * (2 ** (attempt - 1)))
    raise RuntimeError(f"OMDb request failed after {retries} retries: {last_err}") from last_err


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = ["Row", "Title", "Year", "Genre", "imdbRating", "Actors (Main)", "BoxOffice"]
    for c in required:
        if c not in df.columns:
            df[c] = ""
    extras = [c for c in df.columns if c not in required]
    return df[required + extras]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("input_csv")
    ap.add_argument("output_csv")
    ap.add_argument("--xlsx", default="")
    ap.add_argument("--cache", default="omdb_cache.json")
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--retries", type=int, default=3)
    args = ap.parse_args()

    api_key = os.getenv("OMDB_API_KEY", "").strip()
    if not api_key:
        print("ERROR: OMDB_API_KEY is not set.", file=sys.stderr)
        return 2

    df = pd.read_csv(args.input_csv)
    df = ensure_columns(df)
    # Force target columns to be text to avoid dtype conflicts (float vs string)
    text_cols = ["Year", "Genre", "imdbRating", "Actors (Main)", "BoxOffice"]
    for c in text_cols:
        df[c] = df[c].astype("string")
    
    df["Row"] = pd.to_numeric(df["Row"], errors="coerce").fillna(0).astype(int)


    cache = load_cache(args.cache)

    total = len(df)
    cache_hits = 0
    api_calls = 0

    for i, row in df.iterrows():
        title = str(row.get("Title", "")).strip()
        #print(f"  row={row}\n")
        if not title or title.lower() == "nan":
            continue
        
        year = str(row.get("Year", "")).strip()
        if year.lower() == "nan":
            year = ""

        key = make_key(title, year)

        if key in cache:
            data = cache[key]
            cache_hits += 1
        else:
            params = {"t": title, "plot": "short", "r": "json"}
            if year:
                params["y"] = year

            try:
                data = omdb_get(params, api_key, retries=args.retries)
                print (f"data= {data}")
            except Exception as e:
                df.at[i, "imdbRating"] = f"ERROR: {e}"
                continue

            cache[key] = data
            save_cache(args.cache, cache)
            api_calls += 1
            time.sleep(args.sleep)

        if not isinstance(data, dict) or data.get("Response") != "True":
            df.at[i, "imdbRating"] = f"NOT FOUND: {(data or {}).get('Error', 'Unknown')}"
            continue

        if not year:
            df.at[i, "Year"] = data.get("Year", "") or df.at[i, "Year"]
        df.at[i, "Genre"] = data.get("Genre", "") or df.at[i, "Genre"]
        df.at[i, "imdbRating"] = data.get("imdbRating", "") or df.at[i, "imdbRating"]
        df.at[i, "Actors (Main)"] = data.get("Actors", "") or df.at[i, "Actors (Main)"]
        df.at[i, "BoxOffice"] = data.get("BoxOffice", "") or df.at[i, "BoxOffice"]

        df.at[i, "Row"] = i + 1

        if (i + 1) % 25 == 0:
            print(f"Processed {i+1}/{total} (cache hits: {cache_hits}, API calls: {api_calls})")

    df.to_csv(args.output_csv, index=False)

    if args.xlsx:
        df.to_excel(args.xlsx, index=False, sheet_name="Movies")

    print(f"Done: {args.output_csv}")
    if args.xlsx:
        print(f"Also wrote: {args.xlsx}")
    print(f"Cache hits: {cache_hits}, API calls: {api_calls}, cache: {args.cache}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
