import re, time, random, functools
from pathlib import Path

def clean_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", name)[:50]

def retry(times: int = 3, backoff: float = 1.5):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(1, times + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == times:
                        raise
                    sleep = backoff ** i + random.uniform(0, 0.5)
                    time.sleep(sleep)
            return None
        return wrapper
    return decorator