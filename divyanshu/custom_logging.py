
import logging
import env

logger = logging.getLogger(__name__)

logger.setLevel(env.LOG_LEVEL)

formatter = logging.Formatter(env.LOG_FORMAT)

file_handler = logging.FileHandler(env.LOG_FILE, mode = 'w')

file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

