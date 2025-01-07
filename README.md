
# Proyecto de Análisis de Base de Datos

## Descripción

Este proyecto tiene como objetivo realizar un análisis descriptivo de una base de datos utilizando Python. Se establecen conexiones a la base de datos para obtener datos de las tablas, procesarlas en formato pandas.DataFrame, guardarlas en formato parquet y realizar visualizaciones relevantes.

## Requisitos Previos

### Instalación de Librerías

Asegúrate de instalar las siguientes librerías necesarias para el proyecto:
´

    pip install pyarrow

    pip install pandas

    pip install fastparquet

    pip install pyodbc sqlalchemy

## Estructura del Proyecto

El proyecto está compuesto por dos archivos principales:

- database_manager.py: Contiene la clase DatabaseManager, que maneja la conexión a la base de datos y los métodos para realizar las tareas requeridas.

- main.py: Archivo principal que utiliza la clase DatabaseManager para ejecutar las funcionalidades del proyecto.

La estructura del proyecto es la siguiente:

    ├── files # Carpeta autogenerada para exportación de parquets
    ├── database_manager.py  # Clase para la gestión de la base de datos
    ├── main.py              # Archivo principal para ejecutar el proyecto
    ├── README.md  