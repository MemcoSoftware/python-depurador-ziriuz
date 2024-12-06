from db import get_db_connection

try:
    # Obtener la conexión desde db.py
    connection = get_db_connection()
    cursor = connection.cursor()

    # Consulta para leer todos los datos de la tabla modelos
    query = "SELECT * FROM modelos;"
    cursor.execute(query)
    
    # Recuperar los resultados de la consulta
    resultados = cursor.fetchall()

    # Mostrar los resultados en la consola
    print("Datos de la tabla 'modelos':")
    for fila in resultados:
        print(fila)

    # Cerrar el cursor y la conexión
    cursor.close()
    connection.close()

except Exception as e:
    print(f"Error al leer la tabla 'modelos': {e}")
