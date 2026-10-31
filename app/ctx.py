class Ctx:
    """Process-wide handles filled in by the lifespan hook in main.py."""

    primary = None
    replica = None
    redis = None


ctx = Ctx()
