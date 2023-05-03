import logging
import logging.config
from logging import getLogger

logging.config.fileConfig('logging.ini', disable_existing_loggers=False)
logger = getLogger(__name__)

# Log some messages
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger.debug('This is a debug message')
logger.info('This is an info message')
logger.warning('This is a warning message')
logger.error('This is an error message')
logger.critical('This is a critical message')
