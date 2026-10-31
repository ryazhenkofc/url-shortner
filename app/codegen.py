import re
import secrets
import string
from urllib.parse import urlsplit

from .errors import BadRequest

ALPHABET = string.ascii_uppercase + string.ascii_lowercase + string.digits   # 62 symbols
ALIAS_RE = re.compile(r"^[A-Za-z0-9_-]{3,10}$")
RESERVED = {"api", "admin", "health", "metrics", "static"}


def new_code(n=7):
    return "".join(secrets.choice(ALPHABET) for _ in range(n))


def validate_alias(alias):
    if not ALIAS_RE.fullmatch(alias) or alias.lower() in RESERVED:
        raise BadRequest("bad alias")


def validate_url(url, own_host, blocklist):
    if len(url) > 2048:
        raise BadRequest("url too long")
    if any(ord(c) < 32 or ord(c) == 127 for c in url):
        raise BadRequest("bad url")
    try:
        url.encode()
        p = urlsplit(url)
    except ValueError:                 # malformed IPv6 host or a lone surrogate
        raise BadRequest("bad url")
    if p.scheme not in ("http", "https") or not p.hostname:
        raise BadRequest("bad url")
    if p.hostname == own_host or p.hostname in blocklist:
        raise BadRequest("host not allowed")
