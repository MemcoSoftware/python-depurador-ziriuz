import pymysql
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Configuración de la conexión
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': int(os.getenv('DB_PORT'))
}

def get_db_connection():
    """
    Función para obtener la conexión a la base de datos.
    """
    try:
        connection = pymysql.connect(**db_config)
        print("Conexión exitosa a la base de datos")
        return connection
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None
