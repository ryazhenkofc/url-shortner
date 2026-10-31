BASE_URL ?= $(shell grep ^BASE_URL .env 2>/dev/null | cut -d= -f2)

.PHONY: up down reset logs ps test test-int seed codes load obs

up:        ; docker compose up -d --build
down:      ; docker compose down
reset:     ; docker compose down -v          # drop volumes (fresh DB init)
logs:      ; docker compose logs -f app1 app2 app3
ps:        ; docker compose ps
test:      ; pytest tests/unit
test-int:  ; BASE_URL=$(BASE_URL) pytest tests/integration   # against a running compose
seed:      ; docker compose exec -T pg-primary psql -U shortener -d shortener < load/seed.sql
codes:     ; docker compose exec -T pg-replica psql -U shortener -d shortener -At \
               -c "SELECT json_agg(code) FROM links WHERE deleted_at IS NULL AND (expires_at IS NULL OR expires_at > now())" > load/codes.json
load:      ; docker compose --profile load run --rm k6
obs:       ; docker compose --profile obs up -d
