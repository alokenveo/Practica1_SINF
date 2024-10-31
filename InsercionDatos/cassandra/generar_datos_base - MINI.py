from cassandra.cluster import Cluster
from faker import Faker
import random
from collections import Counter
from datetime import datetime, timedelta

# Crear una lista de productos genéricos por categoría
def get_suffix(n):
    """Convierte un número entero en un sufijo alfabético."""
    suffix = ""
    while n > 0:
        n -= 1  # Ajustar para el índice basado en 0
        suffix = chr(65 + (n % 26)) + suffix  # 65 es el valor ASCII de 'A'
        n //= 26  # Dividir por 26 para obtener el siguiente carácter
    return suffix

try:
    # Conectar a la base de datos
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('marketing')

    print("Conexión establecida con la base de datos 'marketing'.")

    # Inicializar Faker para generar nombres de clientes
    faker = Faker()

    # Crear una lista de productos genéricos por categoría
    productos_por_categoria = {
        "Electrónica": ["Laptop", "Smartphone", "Cámara", "Televisor", "Auriculares"],
        "Ropa": ["Vestido", "Chaqueta", "Zapatillas", "Camisa", "Pantalones"],
        "Hogar": ["Sofá", "Mesa", "Cafetera", "Fregadero", "Lámpara"],
        "Deportes": ["Bicicleta", "Pelota", "Raqueta", "Botella de agua", "Zapatillas de correr"],
        "Jardinería": ["Tijeras de podar", "Regadera", "Kit de jardinería", "Tierra para macetas", "Semillas"],
        "Juguetes": ["Muñeca", "Bloques de construcción", "Juego de mesa", "Pelota de playa", "Puzzles"]
    }

    # Mantener un conteo de cada nombre de producto base para asignar el sufijo apropiado
    producto_count = {}

    # Generar exactamente 100 productos
    num_productos = 100
    productos = []
    for i in range(num_productos):
        producto_id = i+1
        categoria = random.choice(list(productos_por_categoria.keys()))
        nombre_producto_base = random.choice(productos_por_categoria[categoria])  # Escoge un nombre de producto de la categoría

        # Verificar cuántas veces ya ha aparecido este nombre base
        if nombre_producto_base in producto_count:
            producto_count[nombre_producto_base] += 1
        else:
            producto_count[nombre_producto_base] = 1

        # Generar nombre único con sufijo solo si ya ha aparecido antes
        nombre_producto = f"{nombre_producto_base} {get_suffix(producto_count[nombre_producto_base])}"

        # Generar precio aleatorio entre 10 y 1000 con 2 decimales
        precio = round(random.uniform(10.0, 1000.0), 2)
        # Generar calificación aleatoria entre 0.0 y 5.0
        calificacion = round(random.uniform(0.0, 5.0), 1)

        productos.append((producto_id, nombre_producto, categoria, precio, calificacion))

        # Insertar producto en la tabla 'productos'
        query_insert_producto = "INSERT INTO productos (producto_id, nombre, categoria, precio, calificacion) VALUES (%s, %s, %s, %s, %s)"
        session.execute(query_insert_producto, (producto_id, nombre_producto, categoria, precio, calificacion))

        # Mostrar el progreso en la misma línea
        print(f"\rClientes añadidos: {i + 1}/{num_productos}", end='')

    # Generar un número aleatorio de clientes entre 50 y 75
    num_clientes = random.randint(45, 60)
    print(f"Se han creado un total de {num_clientes} clientes")
    clientes = []

    # Insertar clientes en la tabla 'clientes'
    for i in range(num_clientes):
        cliente_id = i+1
        nombre_cliente = faker.name()

        clientes.append((cliente_id, nombre_cliente))

        # Insertar cliente en la tabla 'clientes'
        query_insert_cliente = "INSERT INTO clientes (cliente_id, nombre) VALUES (%s, %s)"
        session.execute(query_insert_cliente, (cliente_id, nombre_cliente))

    # Contar la cantidad de compras por producto
    cantidad_compras = Counter()

    # Crear un diccionario para contar las compras por cliente
    cantidad_compras_cliente = {cliente[0]: 0 for cliente in clientes}

    # Generar 500 compras aleatorias
    cant_compras = 500
    for _ in range(cant_compras):
        cliente_id, nombre_cliente = random.choice(clientes)
        producto_id, nombre_producto, categoria, precio, calificacion = random.choice(productos)
        fecha_compra = datetime.now() - timedelta(days=random.randint(0, 1825))  # Hasta 5 años atrás

        # Insertar en productos_por_cliente
        query_productos_por_cliente = "INSERT INTO productos_por_cliente (cliente_id, fecha_compra, producto_id, nombre_producto, categoria) VALUES (%s, %s, %s, %s, %s)"
        session.execute(query_productos_por_cliente,
                        (cliente_id, fecha_compra, producto_id, nombre_producto, categoria))

        # Insertar en clientes_por_producto
        query_clientes_por_producto = "INSERT INTO clientes_por_producto (producto_id, fecha_compra, cliente_id, nombre_cliente) VALUES (%s, %s, %s, %s)"
        session.execute(query_clientes_por_producto, (producto_id, fecha_compra, cliente_id, nombre_cliente))

        # Aumentar el conteo de compras para el cliente
        cantidad_compras_cliente[cliente_id] += 1
        cantidad_compras[producto_id]+=1


    # Insertar en productos_mas_comprados_por_categoria
    for producto_id, num_compras in cantidad_compras.items():
        # Obtener el nombre y la categoría del producto
        for nombre_producto, categoria in [(p[1], p[2]) for p in productos if p[0] == producto_id]:
            query_productos_mas_comprados = "INSERT INTO productos_mas_comprados_por_categoria (categoria, producto_id, nombre_producto, num_compras) VALUES (%s, %s, %s, %s)"
            session.execute(query_productos_mas_comprados, (categoria, producto_id, nombre_producto, num_compras))

    # Insertar en recomendaciones_por_cliente
    for _ in range(cant_compras):
        cliente_id, nombre_cliente = random.choice(clientes)
        producto_id, nombre_producto, categoria, precio, calificacion = random.choice(productos)
        fecha_compra = datetime.now() - timedelta(days=random.randint(0, 1825))  # Hasta 5 años atrás
        cliente_similar_id = random.choice(clientes)[0]  # Otro cliente

        query_recomendaciones = "INSERT INTO recomendaciones_por_cliente (cliente_id, producto_id, nombre_producto, categoria, fecha_compra, cliente_similar_id) VALUES (%s, %s, %s, %s, %s, %s)"
        session.execute(query_recomendaciones, (cliente_id, producto_id, nombre_producto, categoria, fecha_compra, cliente_similar_id))

    print("Tablas rellenadas con éxito.")
except Exception as e:
    print("Error al conectar:", e)
finally:
    # Asegúrate de cerrar la conexión si se estableció
    if 'session' in locals():
        session.shutdown()
    cluster.shutdown()



