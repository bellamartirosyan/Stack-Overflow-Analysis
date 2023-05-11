import os
import logging
from configs import log_folder, infrstr_log_file

logfolder = os.getcwd() + '\\' + log_folder
if not os.path.exists(logfolder):
    os.makedirs(logfolder)

logging.basicConfig(
    level=logging.INFO,
    filename=os.path.join(logfolder, infrstr_log_file),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
