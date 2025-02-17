from cassandra.cluster import Cluster
from collections import defaultdict

try:
    # Conectar a la base de datos
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('marketing')
    print("Conexión establecida con la base de datos 'marketing'.")

    # Obtener los productos de la tabla 'productos'
    query = "SELECT producto_id, nombre, categoria, precio FROM productos"
    rows = session.execute(query)
    
    # Agrupar productos por categoría y obtener los más caros
    productos_por_categoria = defaultdict(list)
    for row in rows:
        productos_por_categoria[row.categoria].append((row.producto_id, row.nombre, row.precio))
    
    # Insertar los productos más caros en la nueva tabla
    for categoria, productos in productos_por_categoria.items():
        productos.sort(key=lambda x: (-x[2], x[0]))  # Ordenar por precio DESC, producto_id ASC
        for producto_id, nombre_producto, precio in productos:
            query = """
            INSERT INTO productos_mas_caros_por_categoria (categoria, producto_id, nombre_producto, precio)
            VALUES (%s, %s, %s, %s)
            """
            session.execute(query, (categoria, producto_id, nombre_producto, precio))
    
    print("\nTabla 'productos_mas_caros_por_categoria' rellenada con éxito.")

except Exception as e:
    print("Error al conectar:", e)

finally:
    if 'session' in locals():
        session.shutdown()
    cluster.shutdown()