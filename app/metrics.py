from prometheus_client import Counter, Histogram

REQ_DURATION = Histogram("http_request_duration_seconds", "Request latency", ["route", "status"],
                         buckets=(.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5))
HITS = Counter("cache_hits_total", "Resolved lookups by layer", ["layer"])
OVERLOAD_REJECTS = Counter("overload_rejects_total", "Requests rejected by a guard")
DB_ERRORS = Counter("db_errors_total", "Database errors", ["pool"])
REDIS_ERRORS = Counter("redis_errors_total", "Redis errors and timeouts")
