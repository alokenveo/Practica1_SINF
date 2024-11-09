from cassandra.cluster import Cluster
from faker import Faker
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta

# Convertir un número entero en un sufijo alfabético
def get_suffix(n):
    suffix = ""
    while n > 0:
        n -= 1
        suffix = chr(65 + (n % 26)) + suffix
        n //= 26
    return suffix

try:
    # Conectar a la base de datos
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('marketing')
    print("Conexión establecida con la base de datos 'marketing'.")

    # Inicializar Faker y definir productos
    faker = Faker()
    productos_por_categoria = {
        "Electrónica": ["Laptop", "Smartphone", "Cámara", "Televisor", "Auriculares"],
        "Ropa": ["Vestido", "Chaqueta", "Zapatillas", "Camisa", "Pantalones"],
        "Hogar": ["Sofá", "Mesa", "Cafetera", "Fregadero", "Lámpara"],
        "Deportes": ["Bicicleta", "Pelota", "Raqueta", "Botella de agua", "Zapatillas de correr"],
        "Jardinería": ["Tijeras de podar", "Regadera", "Kit de jardinería", "Tierra para macetas", "Semillas"],
        "Juguetes": ["Muñeca", "Bloques de construcción", "Juego de mesa", "Pelota de playa", "Puzzles"]
    }

    producto_count = Counter()  # Conteo de productos base para sufijos
    productos = []

    # Generar y almacenar 1000 productos
    for i in range(1000):
        categoria = random.choice(list(productos_por_categoria.keys()))
        nombre_base = random.choice(productos_por_categoria[categoria])

        # Crear nombre único si se repite y actualizar el contador
        producto_count[nombre_base] += 1
        nombre_producto = f"{nombre_base} {get_suffix(producto_count[nombre_base])}"

        # Generar precio y calificación aleatorios
        precio = round(random.uniform(10.0, 1000.0), 2)
        calificacion = round(random.uniform(0.0, 5.0), 1)
        productos.append((i + 1, nombre_producto, categoria, precio, calificacion))

        # Insertar producto en 'productos'
        query = "INSERT INTO productos (producto_id, nombre, categoria, precio, calificacion) VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (i + 1, nombre_producto, categoria, precio, calificacion))
        print(f"\rProductos creados: {i + 1}/1000", end='')

    # Generar clientes y sus compras
    clientes = [(i + 1, faker.name()) for i in range(random.randint(45, 60))]
    for cliente_id, nombre_cliente in clientes:
        query = "INSERT INTO clientes (cliente_id, nombre) VALUES (%s, %s)"
        session.execute(query, (cliente_id, nombre_cliente))

    # Contador de compras por producto y cliente
    cantidad_compras = Counter()
    compras_por_cliente = defaultdict(set)

    # Crear 5000 compras
    for _ in range(5000):
        cliente_id, _ = random.choice(clientes)
        producto = random.choice(productos)
        fecha_compra = datetime.now() - timedelta(days=random.randint(0, 1825))

        # Insertar compra en las tablas
        session.execute(
            "INSERT INTO productos_por_cliente (cliente_id, fecha_compra, producto_id, nombre_producto, categoria) VALUES (%s, %s, %s, %s, %s)",
            (cliente_id, fecha_compra, producto[0], producto[1], producto[2])
        )
        session.execute(
            "INSERT INTO clientes_por_producto (producto_id, fecha_compra, cliente_id, nombre_cliente) VALUES (%s, %s, %s, %s)",
            (producto[0], fecha_compra, cliente_id, nombre_cliente)
        )

        # Registrar compras
        cantidad_compras[producto[0]] += 1
        compras_por_cliente[cliente_id].add(producto[0])

    # Insertar productos más comprados por categoría
    for producto_id, num_compras in cantidad_compras.items():
        nombre_producto, categoria = next((p[1], p[2]) for p in productos if p[0] == producto_id)
        query = "INSERT INTO productos_mas_comprados_por_categoria (categoria, producto_id, nombre_producto, num_compras) VALUES (%s, %s, %s, %s)"
        session.execute(query, (categoria, producto_id, nombre_producto, num_compras))

    # Crear recomendaciones basadas en cliente similar
    for cliente_id, productos_comprados in compras_por_cliente.items():
        productos_no_vistos = None
        for posible_similar, productos_similares in compras_por_cliente.items():
            if posible_similar != cliente_id:
                productos_no_vistos = productos_similares - productos_comprados
                if productos_no_vistos:
                    producto_recomendado_id = productos_no_vistos.pop()
                    nombre_producto, categoria = next((p[1], p[2]) for p in productos if p[0] == producto_recomendado_id)
                    query = "INSERT INTO recomendaciones_por_cliente (cliente_id, producto_id, nombre_producto, categoria, fecha_compra, cliente_similar_id) VALUES (%s, %s, %s, %s, %s, %s)"
                    session.execute(query, (cliente_id, producto_recomendado_id, nombre_producto, categoria, datetime.now(), posible_similar))
                    break  # Se realiza una sola recomendación

    print("\nTablas rellenadas con éxito.")

except Exception as e:
    print("Error al conectar:", e)

finally:
    if 'session' in locals():
        session.shutdown()
    cluster.shutdown()
