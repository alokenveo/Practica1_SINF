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
    num_productos=10000

    # Generar y almacenar 10000 productos
    for i in range(num_productos):
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
        print(f"\rProductos creados: {i + 1}/{num_productos}", end='')
    print("\n")

    # Generar clientes y sus compras
    num_clientes=random.randint(4500, 6000)
    clientes = [(i + 1, faker.name()) for i in range(num_clientes)]
    for cliente_id, nombre_cliente in clientes:
        query = "INSERT INTO clientes (cliente_id, nombre) VALUES (%s, %s)"
        session.execute(query, (cliente_id, nombre_cliente))
    print(f"Se han introducido {num_clientes} clientes")

    # Contador de compras por producto y cliente
    cantidad_compras = Counter()
    compras_por_cliente = defaultdict(set)
    cant_compras=50000

    # Crear 50000 compras
    for i in range(cant_compras):
        cliente_id, _ = random.choice(clientes)
        producto = random.choice(productos)
        fecha_compra = datetime.now() - timedelta(days=random.randint(0, 1095))

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
        print(f"\rCompras realizadas: {i + 1}/{cant_compras}", end='')
    print("\n")

    # Insertar productos más comprados por categoría
    for i, (producto_id, num_compras) in enumerate(cantidad_compras.items()):
        nombre_producto, categoria = next((p[1], p[2]) for p in productos if p[0] == producto_id)
        query = "INSERT INTO productos_mas_comprados_por_categoria (categoria, producto_id, nombre_producto, num_compras) VALUES (%s, %s, %s, %s)"
        session.execute(query, (categoria, producto_id, nombre_producto, num_compras))
        print(f"\rProductos más comprados insertados: {i + 1}/{len(cantidad_compras)}", end='')
    print("\n")

    # Crear recomendaciones basadas en cliente similar
    # Diccionario para almacenar productos por cliente
    productos_por_cliente = defaultdict(set)
    productos_base_por_cliente = defaultdict(set)

    # Paso 1: Construir datos base
    for cliente_id, compras in compras_por_cliente.items():
        for producto_id in compras:
            producto_data = next(p for p in productos if p[0] == producto_id)
            producto_base = producto_data[1].split(" ")[0]  # Extraer nombre base
            productos_por_cliente[cliente_id].add(producto_id)
            productos_base_por_cliente[cliente_id].add(producto_base)

    # Paso 2: Generar recomendaciones
    for cliente_id, nombres_base_actual in productos_base_por_cliente.items():
        if not nombres_base_actual:
            continue  # Si el cliente no tiene compras, pasamos al siguiente

        # Buscar un cliente similar
        cliente_similar_id = None
        productos_cliente_similar = set()
        for otro_cliente_id, nombres_base_otro in productos_base_por_cliente.items():
            if cliente_id == otro_cliente_id:
                continue  # No comparar con el mismo cliente
            if nombres_base_actual.issubset(nombres_base_otro):
                productos_cliente_similar = productos_por_cliente[otro_cliente_id]
                cliente_similar_id = otro_cliente_id
                break

        if not cliente_similar_id:
            continue  # Si no hay cliente similar, pasamos al siguiente

        # Identificar productos a recomendar
        productos_a_recomendar = productos_cliente_similar - productos_por_cliente[cliente_id]
        for producto_id in productos_a_recomendar:
            producto_data = next(p for p in productos if p[0] == producto_id)
            fecha_compra = datetime.now() - timedelta(days=random.randint(0, 1095))
            query = """
            INSERT INTO marketing.recomendaciones_por_cliente (cliente_id, fecha_compra, producto_id, categoria, cliente_similar_id, nombre_producto)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            session.execute(query, (
                cliente_id,
                fecha_compra,
                producto_id,
                producto_data[2],  # Categoría
                cliente_similar_id,
                producto_data[1]  # Nombre del producto
            ))

    print("\nTablas rellenadas con éxito.")

except Exception as e:
    print("Error al conectar:", e)

finally:
    if 'session' in locals():
        session.shutdown()
    cluster.shutdown()
