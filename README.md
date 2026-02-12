Welcome! This repo tracks Reform UK councillors and their profile pages, and (optionally) scrapes registers of interests later.

# reform_register_scraper

Minimal tooling to load councillor seed data, discover Reform UK councillor profile pages, and optionally scrape registers.

## Setup

1. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

2. Configure the database connection using environment variables:

- `DB_HOST` (default: `localhost`)
- `DB_PORT` (default: `5432`)
- `DB_NAME` (default: `reform_register`)
- `DB_USER` (default: `postgres`)
- `DB_PASSWORD` (default: `postgres`)

3. Create the schema:

```bash
psql -d "$DB_NAME" -f db/schema.sql
```

If you already created the database, add the homepage cache table:

```bash
psql -d "$DB_NAME" -c "CREATE TABLE IF NOT EXISTS council_homepages (council TEXT PRIMARY KEY, homepage_url TEXT NOT NULL, discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW());"
```

## Usage

Load councillors from the CSV file (`reform-councillors.csv` in the repo root). The CSV must include `council`, `ward`, and `name` columns.

```bash
python scripts/load_csv.py
```

Build/update the Reform councillor profile page database (democracy.gov path). This skips councils already logged and updates `missing_councillors.csv` and `missing-councils.csv`:

```bash
USE_DEMOCRACY=1 python scripts/find_reform_councillors.py
```

Scrape registers and store results (optional, for later):

```bash
USE_HOMEPAGE_CRAWL=0 USE_FALLBACK_SEARCH=0 python -m scripts.scrape_registers
```

## Search UI

Run a simple local web app to search results:

```bash
python app.py
```

Then open `http://127.0.0.1:5000` in your browser.
