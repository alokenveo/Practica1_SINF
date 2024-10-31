from bson import ObjectId
from pymongo import MongoClient
from faker import Faker
import random

fake = Faker()

# Variable global para mantener un contador de IDs enteros
producto_id_counter = 1
cliente_id_counter = 1

def generar_historial_compras():
    """Genera un historial de compras aleatorio."""
    global producto_id_counter
    historial = []
    for _ in range(random.randint(1, 21)):  # Entre 1 y 20 compras
        compra = {
            "producto_id": producto_id_counter,  # Usar un ID entero secuencial para el producto
            "nombre_producto": fake.word().capitalize(),
            "precio": round(random.uniform(10.0, 500.0), 2),  # Precio aleatorio entre 10 y 500
            "fecha_compra": fake.date_time_this_year().isoformat() + 'Z',
            "categoria": random.choice(["Electrónica", "Deportes", "Hogar", "Moda"]),
            "calificacion": round(random.uniform(1, 5), 1)  # Calificación aleatoria entre 1 y 5
        }
        historial.append(compra)
        producto_id_counter += 1  # Incrementar el contador de producto_id
    return historial

def generar_historial_interacciones():
    """Genera un historial de interacciones aleatorio."""
    return {
        "ultima_visita": fake.date_time_this_year().isoformat() + 'Z',
        "busquedas": [fake.word() for _ in range(random.randint(1, 3))],  # Mantener búsquedas aleatorias de productos
        "clics": random.randint(1, 10)  # Número aleatorio de clics en la página
    }

def insertar_datos(client, base_datos, coleccion, cantidad):
    """Inserta documentos aleatorios en la colección especificada."""
    global cliente_id_counter
    db = client[base_datos]
    collection = db[coleccion]

    for i in range(cantidad):
        edad = random.randint(18, 70)  # Edad aleatoria entre 18 y 70 años
        documento = {
            "_id": cliente_id_counter,  # Usar un ID entero secuencial para el cliente
            "info": {
                "nombre": fake.name(),
                "correo": fake.email(),
                "edad": edad
            },
            "historial_compras": generar_historial_compras(),
            "preferencias_productos": random.sample(["Electrónica", "Deportes", "Hogar", "Moda", "Libros"], k=2),
            "historial_interacciones_web": [generar_historial_interacciones()],
            "localizacion": {
                "ciudad": fake.city(),
                "pais": fake.country()
            },
            "metodos_pago": random.sample(["Tarjeta de crédito", "PayPal", "Transferencia bancaria", "Criptomonedas"], k=2)
        }
        collection.insert_one(documento)

        # Mostrar el progreso en la misma línea
        print(f"\rClientes añadidos: {i + 1}/{cantidad}", end='')

        cliente_id_counter += 1  # Incrementar el contador de cliente_id

    print(f'Se han insertado {cantidad} documentos en la colección {coleccion}.')


if __name__ == '__main__':
    # Conectar al cliente de MongoDB
    client = MongoClient('mongodb://localhost:27017/')

    # Insertar 10 documentos aleatorios
    insertar_datos(client, 'marketing', 'clientes', 50000)
    client.close()
