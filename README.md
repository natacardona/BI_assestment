
# Proyecto de Análisis de Base de Datos

## Descripción

Este proyecto tiene como objetivo realizar un análisis descriptivo de una base de datos utilizando Python. Se establecen conexiones a la base de datos para obtener datos de las tablas, procesarlas en formato pandas.DataFrame, guardarlas en formato parquet y realizar visualizaciones relevantes.

## Requisitos Previos

### Instalación de Librerías

Asegúrate de instalar las siguientes librerías necesarias para el proyecto:

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

Funcionalidades Principales:

#### Clase DatabaseManager

- La clase proporciona las siguientes funcionalidades:

#### Conexión a la Base de Datos

- Crea y cierra la conexión con la base de datos mediante SQLAlchemy.

#### Guardar Tablas en Formato Parquet

- Guarda todas las tablas de la base de datos en archivos Parquet para almacenamiento local.
´
    db_manager.save_tables_to_parquet(output_dir="parquet_tables")
´
#### Análisis de Clientes

- Agrupa y cuenta los clientes por país y muestra los 5 países con más clientes.

´  
    db_manager.group_and_display_customers_by_country()
´

#### Análisis de Ventas

Realiza un análisis completo de las tablas Invoice y InvoiceLine:

- Identifica los 5 tracks más vendidos.

- Determina los 5 artistas con mayores ventas.

- Encuentra el mes con más ventas.

- Genera un top 5 de los géneros más vendidos.

´
    db_manager.analyze_sales()
´
