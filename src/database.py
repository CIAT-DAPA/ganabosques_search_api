from mongoengine import connect
from dotenv import load_dotenv
import os
from src.tools.logger import logger

load_dotenv()


DATABASE_URL = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("MONGO_DB_NAME")
logger.info(f"Mongo database: {DATABASE_NAME}")
def init_db():
    conn = connect(
        db=DATABASE_NAME,
        host=DATABASE_URL,
        alias="default"
    )

    # Forzar verificación de conexión
    conn.admin.command("ping")