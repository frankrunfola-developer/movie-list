# Movie Enrichment (OMDb)

Enrich a movie CSV using the OMDb API.

Fills these columns (when available):
- Year
- Genre
- imdbRating
- Actors (Main)
- BoxOffice

## Virtual environment

### macOS / Linux
```bash
# 1) create the venv
python -m venv .venv

# 2) activate it (Git Bash uses this path)
source .venv/Scripts/activate

# 3) install deps
python -m pip install --upgrade pip
pip install pandas requests openpyxl
```

### Windows (PowerShell)
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install pandas requests openpyxl
```

## OMDb API key

Get a key: https://www.omdbapi.com/apikey.aspx

Set it:



### Windows (PowerShell)
```powershell
setx OMDB_API_KEY "7ff6c487"
# reopen terminal after running setx
```

### macOS / Linux
```bash
export OMDB_API_KEY="7ff6c487"
```

## Run

```bash
python enrich_movies_omdb_csv.py movies_in_small.csv enriched_small.csv
python enrich_movies_omdb_csv.py movies_in_large.csv enriched_large.csv
```

Optional Excel output:

```bash
python enrich_movies_omdb_csv.py movies_in_small.csv enriched.csv --xlsx enriched_small.xlsx
python enrich_movies_omdb_csv.py movies_in_large.csv enriched.csv --xlsx enriched_large.xlsx
```

## Useful flags

- `--cache` cache file (default: `omdb_cache.json`)
- `--sleep` delay between calls (default: `0.25`)
- `--retries` retries per request (default: `3`)
