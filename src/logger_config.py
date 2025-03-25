import logging
import os
from datetime import datetime

def setup_logger():
    """Configure logging for the application"""
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Create log filename with timestamp
    log_file = os.path.join(log_dir, f'easysql_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # Also print to console
        ]
    )

    return logging.getLogger(__name__)