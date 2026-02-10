welcome to my vibecoding adventure in scraping the register of interest of reform councillors. check back here later for instructions on how to use.

# reform_register_scraper

Minimal tooling to load councillor seed data, crawl council sites for register pages, and store register documents.

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

Scrape registers and store results (the scraper finds the council homepage via web search, crawls for register pages/PDFs, then matches councillor names in those pages):

```bash
python scripts/scrape_registers.py
```
