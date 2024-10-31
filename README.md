# Práctica de Integración de Bases de Datos: Cassandra y MongoDB

## Descripción

Esta práctica tiene como objetivo integrar y gestionar datos utilizando dos bases de datos NoSQL: **Cassandra** y **MongoDB**. Se han desarrollado scripts en **Python** para poblar ambas bases de datos y aplicaciones en **Java** para interactuar con estos sistemas, facilitando la consulta y el análisis de datos.

## Estructura del Proyecto

El proyecto se divide en las siguientes partes:

1. **Scripts de Población de Bases de Datos**:
   - **Cassandra**: `poblar_cassandra.py`
   - **MongoDB**: `poblar_mongodb.py`

2. **Aplicaciones en Java**:
   - **Cassandra**: `AccesoCassandra`
   - **MongoDB**: `AccesoMongoDB`

### Scripts de Python

#### 1. `generar_datos_base.py`
Este script se encarga de insertar datos en la base de datos Cassandra. La estructura de los datos incluye información de clientes, productos y compras, que se utilizan para realizar consultas posteriores.

#### 2. `insertar_datos.py`
Este script inserta documentos en MongoDB. Los documentos contienen información similar a la de Cassandra, permitiendo realizar análisis de datos y consultas específicas.

### Aplicaciones en Java

#### 1. `AccesoCassandra`
Desarrollada en **Eclipse**, esta aplicación permite conectarse a la base de datos Cassandra y realizar consultas sobre los datos de productos y clientes. Las consultas implementadas incluyen:
- Listar productos comprados por un cliente.
- Obtener total de productos comprados por un cliente en un rango de fechas.

#### 2. `AccesoMongoDB`
También desarrollada en **Eclipse**, esta aplicación facilita la interacción con MongoDB. Las consultas incluyen:
- Contar cuántos clientes han realizado compras en los últimos 7 días.
- Listar clientes que han comprado más de 5 veces en los últimos 6 meses.

## Requisitos

- **Python**: Necesario para ejecutar los scripts de población. Asegúrate de tener instaladas las bibliotecas requeridas:
  ```bash
  pip install cassandra-driver pymongo
