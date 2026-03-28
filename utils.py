import time
from collections import deque
from typing import Dict

# Production-grade rate limiter: max 5 searches per minute per user
RATE_LIMIT_DB: Dict[int, deque] = {}
MAX_REQUESTS = 5
TIME_WINDOW = 60  # seconds


def is_rate_limited(user_id: int) -> bool:
    """
    Production-grade rate limiter allowing max 5 searches per minute per user.
    
    Uses a sliding window approach for accurate rate limiting.
    Prevents Telegram FloodWait errors and bot lag.
    
    Args:
        user_id: Telegram user ID
        
    Returns:
        True if user exceeded rate limit, False if allowed
    """
    now = time.time()
    
    # Initialize user's request queue if not exists
    if user_id not in RATE_LIMIT_DB:
        RATE_LIMIT_DB[user_id] = deque()
    
    user_queue = RATE_LIMIT_DB[user_id]
    
    # Remove old requests outside the time window
    while user_queue and (now - user_queue[0]) > TIME_WINDOW:
        user_queue.popleft()
    
    # Check if limit reached
    if len(user_queue) >= MAX_REQUESTS:
        return True
    
    # Add current request timestamp
    user_queue.append(now)
    
    # Garbage collection: clean up inactive users occasionally
    if len(RATE_LIMIT_DB) > 50000:
        inactive_users = [
            uid for uid, q in RATE_LIMIT_DB.items() 
            if q and (now - q[-1]) > TIME_WINDOW * 10
        ]
        for uid in inactive_users:
            del RATE_LIMIT_DB[uid]
    
    return False


def get_rate_limit_status(user_id: int) -> dict:
    """
    Get remaining searches for a user in current time window.
    Useful for user feedback.
    """
    now = time.time()
    if user_id not in RATE_LIMIT_DB:
        return {"remaining": MAX_REQUESTS, "reset_in": 0}
    
    user_queue = RATE_LIMIT_DB[user_id]
    
    # Remove old requests outside time window
    while user_queue and (now - user_queue[0]) > TIME_WINDOW:
        user_queue.popleft()
    
    remaining = MAX_REQUESTS - len(user_queue)
    reset_in = max(0, TIME_WINDOW - (now - user_queue[0])) if user_queue else 0
    
    return {
        "remaining": remaining,
        "reset_in": int(reset_in)
    }
