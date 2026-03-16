import time

RATE_LIMIT_DB = {}

def is_rate_limited(user_id: int, limit_seconds: int = 3) -> bool:
    """
    An in-memory rate limiter that allows 1 request per limit_seconds.
    Cleaning up old entries dynamically to prevent memory blowout.
    """
    now = time.time()
    last_request_time = RATE_LIMIT_DB.get(user_id)
    
    # Clean up occasionally (simple garbage collection of dict)
    # in a real 1GB environment, if the dict gets over 500k entries, it might use ~40MB
    # Let's clear out keys older than limit_seconds just periodically
    if len(RATE_LIMIT_DB) > 50000:
        keys_to_delete = [k for k, v in RATE_LIMIT_DB.items() if (now - v) > limit_seconds]
        for k in keys_to_delete:
            del RATE_LIMIT_DB[k]
            
    if last_request_time:
        if now - last_request_time < limit_seconds:
            return True
            
    RATE_LIMIT_DB[user_id] = now
    return False
