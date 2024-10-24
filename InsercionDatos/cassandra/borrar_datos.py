from cassandra.cluster import Cluster

try:
    # Conectar a la base de datos
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('marketing')  # Reemplaza con el nombre de tu keyspace

    # Obtener la lista de tablas en el keyspace
    tables_query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'marketing';"
    tables = session.execute(tables_query)

    # Truncar cada tabla
    for table in tables:
        truncate_query = f"TRUNCATE {table.table_name};"
        session.execute(truncate_query)
        print(f"Truncada la tabla: {table.table_name}")

    print("Datos borrados")
except Exception as e:
    print("Error al conectar:", e)
finally:
    # Asegúrate de cerrar la conexión si se estableció
    if 'session' in locals():
        session.shutdown()
    cluster.shutdown()
