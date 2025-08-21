import time
import logging
from functools import wraps

def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Record the start time
        result = func(*args, **kwargs)  # Call the actual function
        end_time = time.time()  # Record the end time
        elapsed_time = end_time - start_time  # Calculate elapsed time
        # logging.info(f"Execution time for {func.__name__}: {elapsed_time:.4f} seconds")
        # Log the elapsed time in minutes
        logging.info(f"Execution time for {func.__name__}: {elapsed_time / 60:.4f} minutes")
        return result
    return wrapper

