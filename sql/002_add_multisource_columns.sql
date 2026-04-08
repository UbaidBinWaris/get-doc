ALTER TABLE doctors
    ADD COLUMN IF NOT EXISTS source TEXT,
    ADD COLUMN IF NOT EXISTS external_id TEXT,
    ADD COLUMN IF NOT EXISTS npi TEXT,
    ADD COLUMN IF NOT EXISTS specialty TEXT,
    ADD COLUMN IF NOT EXISTS address_line1 TEXT,
    ADD COLUMN IF NOT EXISTS city TEXT,
    ADD COLUMN IF NOT EXISTS state TEXT,
    ADD COLUMN IF NOT EXISTS postal_code TEXT,
    ADD COLUMN IF NOT EXISTS country TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS ux_doctors_source_external
    ON doctors (source, external_id)
    WHERE source IS NOT NULL AND external_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_doctors_npi
    ON doctors (npi);

CREATE INDEX IF NOT EXISTS ix_doctors_specialty
    ON doctors (LOWER(specialty));
