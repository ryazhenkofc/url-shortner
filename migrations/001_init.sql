CREATE TABLE users (
    id          BIGSERIAL PRIMARY KEY,
    email       TEXT UNIQUE NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    key_hash    BYTEA PRIMARY KEY,                 -- sha256(key)
    user_id     BIGINT NOT NULL REFERENCES users(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE links (
    code        VARCHAR(10) PRIMARY KEY,           -- base62, 7 chars, or an alias
    long_url    TEXT NOT NULL,
    owner_id    BIGINT REFERENCES users(id),       -- NULL = anonymous
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ,                       -- NULL = never
    deleted_at  TIMESTAMPTZ                        -- NULL = active
);
CREATE INDEX idx_links_owner   ON links (owner_id, created_at DESC);
CREATE INDEX idx_links_expires ON links (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE idempotency_keys (
    key              VARCHAR(64) PRIMARY KEY,
    code             VARCHAR(10) NOT NULL,
    response_status  SMALLINT NOT NULL,
    response_body    JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_idem_created ON idempotency_keys (created_at);

-- dev data: two users (the second one is used by the 403 tests)
INSERT INTO users (email) VALUES ('demo@example.com'), ('other@example.com');
INSERT INTO api_keys (key_hash, user_id) VALUES
    (sha256('dev-key-123'::bytea), 1),
    (sha256('dev-key-456'::bytea), 2);
