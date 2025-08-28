import logging
import os
from datetime import datetime

def setup_logging(log_filename="application.log"):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Construct the full log file path
    current_dir = os.path.dirname(os.path.realpath(__file__))
    log_dir = os.path.join(current_dir, '../../logs')
    log_filename = log_filename.replace(".log", "") + f"_{timestamp}.log"
    log_file_path = os.path.join(log_dir, log_filename)
    
    print(f"Logging to: {log_file_path}")
    os.makedirs(log_dir, exist_ok=True)  # Ensure the log directory exists

    # Check if logging has been configured already
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            filename=log_file_path,
            level=logging.INFO,
            format='%(asctime)s - %(message)s'
        )
    return log_filename

if __name__ == "__main__":
    # Call setup_logging with a specific filename
    setup_logging("custom_log")
    logging.info("Logging is set up")
    logging.warning("This is a warning message")
    logging.error("This is an error message")
    logging.info("End of the program")
