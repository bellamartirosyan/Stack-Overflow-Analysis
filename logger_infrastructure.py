import logging
from config import log_folder, log_file_infr
import os


logging.basicConfig(
    level=logging.INFO,
    filename=os.path.join(log_folder, log_file_infr),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
