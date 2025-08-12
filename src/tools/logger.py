""" # logger.py
import logging
from logging.handlers import RotatingFileHandler

# Rotating log handler para evitar archivos gigantes
file_handler = RotatingFileHandler("api.log", maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
))

# Logger raíz
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        file_handler,
        logging.StreamHandler()  # Sigue viendo los logs en consola
    ]
)

stream_handler = logging.StreamHandler()

# También configura loggers específicos de Uvicorn
uvicorn_loggers = ["uvicorn", "uvicorn.error", "uvicorn.access"]
for log_name in uvicorn_loggers:
    logging.getLogger(log_name).handlers = [file_handler, stream_handler]
    logging.getLogger(log_name).setLevel(logging.INFO)
 """

import logging
from logging.handlers import RotatingFileHandler


file_handler = RotatingFileHandler("api.log", maxBytes=5 * 1024 * 1024, backupCount=3)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
))

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(levelname)s:     %(message)s'))

logger = logging.getLogger("ganabosques")
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

for name in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
    uv_logger = logging.getLogger(name)
    uv_logger.addHandler(file_handler)
