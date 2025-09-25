from mongoengine import connect
from dotenv import load_dotenv
import os

load_dotenv()


DATABASE_URL = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("MONGO_DB_NAME")
print(DATABASE_URL)
print(DATABASE_NAME)
def init_db():
    conn = connect(
        db=DATABASE_NAME,
        host=DATABASE_URL,
        alias="default"
    )

    # Forzar verificación de conexión
    conn.admin.command("ping")