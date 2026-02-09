-- Core schema for the reform register scraper.

CREATE TABLE IF NOT EXISTS councillors (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    council TEXT NOT NULL,
    profile_url TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS councillor_registers (
    id SERIAL PRIMARY KEY,
    councillor_id INTEGER NOT NULL REFERENCES councillors(id) ON DELETE CASCADE,
    register_url TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_type TEXT NOT NULL,
    pdf_bytes BYTEA,
    extracted_text TEXT
);

CREATE TABLE IF NOT EXISTS scraping_audit (
    id SERIAL PRIMARY KEY,
    councillor_id INTEGER REFERENCES councillors(id) ON DELETE SET NULL,
    profile_url TEXT,
    issue_type TEXT NOT NULL,
    details TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
