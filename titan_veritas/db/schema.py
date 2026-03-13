"""DDL constants and database initialization for TITAN VERITAS v5.0."""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS original_surname (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE COLLATE NOCASE,
    tier        INTEGER NOT NULL CHECK(tier IN (1, 2, 3)),
    created_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS surname_variant (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    original_surname_id INTEGER NOT NULL REFERENCES original_surname(id),
    variant             TEXT NOT NULL COLLATE NOCASE,
    confidence          REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 100),
    method              TEXT NOT NULL,
    source              TEXT,
    source_url          TEXT,
    created_at          TEXT DEFAULT (datetime('now')),
    UNIQUE(original_surname_id, variant, method)
);

CREATE TABLE IF NOT EXISTS geographic_cluster (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    city             TEXT NOT NULL,
    region           TEXT,
    country          TEXT NOT NULL,
    fratellanza_name TEXT,
    contact_email    TEXT,
    contact_phone    TEXT,
    contact_name     TEXT,
    website_url      TEXT,
    last_scraped_at  TEXT,
    created_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS candidate (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name          TEXT,
    last_name           TEXT NOT NULL,
    known_as            TEXT,
    birth_date          TEXT,
    age                 INTEGER,
    birth_city          TEXT,
    birth_country       TEXT,
    nationalities       TEXT,
    current_club        TEXT,
    current_league      TEXT,
    source              TEXT NOT NULL,
    source_url          TEXT,
    titan_score         INTEGER DEFAULT 0,
    tier                INTEGER,
    score_breakdown     TEXT,
    is_lethal_filtered  INTEGER DEFAULT 0,
    filter_reason       TEXT,
    surname_variant_id  INTEGER REFERENCES surname_variant(id),
    cluster_id          INTEGER REFERENCES geographic_cluster(id),
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS outreach_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id     INTEGER REFERENCES candidate(id),
    cluster_id       INTEGER REFERENCES geographic_cluster(id),
    target_email     TEXT NOT NULL,
    target_name      TEXT,
    email_subject    TEXT,
    email_body_hash  TEXT,
    status           TEXT NOT NULL DEFAULT 'DRAFT'
                     CHECK(status IN ('DRAFT','SENT','DELIVERED','REPLIED','PROCESSING','VALIDATED','DEAD','BOUNCED')),
    sent_at          TEXT,
    replied_at       TEXT,
    gmail_message_id TEXT,
    gmail_thread_id  TEXT,
    llm_extraction   TEXT,
    llm_confidence   REAL,
    telegram_sent    INTEGER DEFAULT 0,
    notes            TEXT,
    created_at       TEXT DEFAULT (datetime('now')),
    updated_at       TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_variant_original ON surname_variant(original_surname_id);
CREATE INDEX IF NOT EXISTS idx_candidate_surname ON candidate(last_name COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_candidate_source ON candidate(source);
CREATE INDEX IF NOT EXISTS idx_outreach_status ON outreach_log(status);
CREATE INDEX IF NOT EXISTS idx_outreach_gmail_thread ON outreach_log(gmail_thread_id);
"""


def init_db(conn):
    """Create all tables and indexes."""
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def seed_surnames(conn):
    """Seed the database with the hardcoded Tier 1 and Tier 2 surname lists."""
    from titan_veritas.core.scoring import TIER_1_NAMES, TIER_2_NAMES

    cursor = conn.cursor()
    for name in TIER_1_NAMES:
        cursor.execute(
            "INSERT OR IGNORE INTO original_surname (name, tier) VALUES (?, 1)",
            (name,),
        )
    for name in TIER_2_NAMES:
        cursor.execute(
            "INSERT OR IGNORE INTO original_surname (name, tier) VALUES (?, 2)",
            (name,),
        )
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM original_surname").fetchone()[0]
    return count


def seed_clusters(conn):
    """Seed known San Marino diaspora geographic clusters (the 25 Fratellanze)."""
    clusters = [
        ("Detroit", "Michigan", "USA", "Fratellanza Sammarinese di Detroit"),
        ("Troy", "Michigan", "USA", "Comunità Sammarinese di Troy"),
        ("New York", "New York", "USA", "Associazione Sammarinese di New York"),
        ("Pergamino", "Buenos Aires", "Argentina", "Fratellanza Sammarinese di Pergamino"),
        ("Córdoba", "Córdoba", "Argentina", "Comunità Sammarinese di Córdoba"),
        ("Buenos Aires", "Buenos Aires", "Argentina", "Associazione Sammarinese di Buenos Aires"),
        ("San Nicolás de los Arroyos", "Buenos Aires", "Argentina", "Comunità Sammarinese di San Nicolás"),
        ("Viedma", "Río Negro", "Argentina", "Fratellanza Sammarinese di Viedma"),
        ("Rosario", "Santa Fe", "Argentina", "Comunità Sammarinese di Rosario"),
        ("Mar del Plata", "Buenos Aires", "Argentina", "Associazione Sammarinese di Mar del Plata"),
        ("Paris", "Île-de-France", "France", "Communauté Saint-Marinaise de Paris"),
        ("Lyon", "Auvergne-Rhône-Alpes", "France", "Association Saint-Marinaise de Lyon"),
        ("Brussels", "Brussels-Capital", "Belgium", "Comunità Sammarinese del Belgio"),
        ("São Paulo", "São Paulo", "Brazil", "Comunidade São-Marinense de São Paulo"),
        ("Rio de Janeiro", "Rio de Janeiro", "Brazil", "Associação São-Marinense do Rio"),
        ("Washington D.C.", "District of Columbia", "USA", "Comunità Sammarinese di Washington"),
        ("Chicago", "Illinois", "USA", "Associazione Sammarinese di Chicago"),
        ("San Francisco", "California", "USA", "Comunità Sammarinese della California"),
        ("Mendoza", "Mendoza", "Argentina", "Fratellanza Sammarinese di Mendoza"),
        ("Montevideo", None, "Uruguay", "Comunità Sammarinese dell'Uruguay"),
        ("London", "England", "UK", "San Marino Society UK"),
        ("Munich", "Bavaria", "Germany", "Comunità Sammarinese di Monaco"),
        ("Zürich", "Zürich", "Switzerland", "Associazione Sammarinese Svizzera"),
        ("Bahía Blanca", "Buenos Aires", "Argentina", "Comunità Sammarinese di Bahía Blanca"),
        ("La Plata", "Buenos Aires", "Argentina", "Fratellanza Sammarinese di La Plata"),
    ]

    cursor = conn.cursor()
    for city, region, country, name in clusters:
        cursor.execute(
            "INSERT OR IGNORE INTO geographic_cluster (city, region, country, fratellanza_name) VALUES (?, ?, ?, ?)",
            (city, region, country, name),
        )
    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM geographic_cluster").fetchone()[0]
    return count
