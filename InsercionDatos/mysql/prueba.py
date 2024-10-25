import mysql.connector
from mysql.connector import Error

def fetch_data():
    try:
        # Conectar a la base de datos
        connection = mysql.connector.connect(
            host='127.0.0.1',    # Dirección del host
            port=3306,            # Puerto de MySQL
            user='etm_admin',     # Usuario de la base de datos
            password='etm_password',  # Cambia esto por tu contraseña
            database='etm_database'  # Nombre de la base de datos
        )

        if connection.is_connected():
            print("Conexión exitosa a la base de datos")
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM prueba")  # Consulta la tabla prueba

            # Recuperar todos los resultados
            records = cursor.fetchall()
            print("Datos en la tabla prueba:")
            for row in records:
                print(row)

    except Error as e:
        print(f"Error al conectarse a la base de datos: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión cerrada.")

if __name__ == "__main__":
    fetch_data()

