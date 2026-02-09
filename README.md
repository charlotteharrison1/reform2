# reform_register_scraper

Minimal tooling to load councillor seed data, locate register links, and store register documents.

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

## Usage

Load councillors from the CSV file:

```bash
python scripts/load_csv.py
```

Scrape registers and store results:

```bash
python scripts/scrape_registers.py
```
