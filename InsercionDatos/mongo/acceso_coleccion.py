from pymongo import MongoClient

def mostrar_documentos(base_datos, coleccion):
    # Conectar al cliente de MongoDB
    client = MongoClient('mongodb://localhost:27017/')  # Cambia la URI si es necesario

    # Seleccionar la base de datos
    db = client[base_datos]

    # Seleccionar la colección
    collection = db[coleccion]

    # Obtener todos los documentos de la colección
    documentos = collection.find()

    # Imprimir cada documento
    for documento in documentos:
        print(documento)

    # Cerrar la conexión
    client.close()


if __name__ == '__main__':
    mostrar_documentos('marketing', 'clientes')
