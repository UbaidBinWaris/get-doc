CREATE TABLE IF NOT EXISTS doctors (
    id BIGSERIAL PRIMARY KEY,
    source_url TEXT NOT NULL,
    doctor_name TEXT NOT NULL,
    clinic_name TEXT,
    phone_number TEXT,
    email TEXT,
    dedupe_hash CHAR(64) NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_doctors_dedupe_hash
    ON doctors (dedupe_hash);

CREATE INDEX IF NOT EXISTS ix_doctors_email
    ON doctors (LOWER(email));

CREATE INDEX IF NOT EXISTS ix_doctors_name
    ON doctors (LOWER(doctor_name));
