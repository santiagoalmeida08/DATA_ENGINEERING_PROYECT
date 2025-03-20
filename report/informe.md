# Nombre: Santiago Almeida
# CC: 1193119436

## 1. Arquitectura de Datos

La arquitectura de datos de esta aplicación sigue el patrón ETL (Extracción, Transformación y Carga), que es un enfoque común para procesar y analizar grandes volúmenes de datos. A continuación, se describen los componentes principales de esta arquitectura:

### 1.1. Extracción de Datos (Extract)
- **Fuente de Datos**: Los datos se extraen de archivos Excel que contienen información sobre películas, inventarios, alquileres, clientes y tiendas.
- **Herramienta de Extracción**: Se utiliza PySpark para leer los archivos Excel y cargar los datos en DataFrames de Spark.
- **Clase Extraccion**: Esta clase se encarga de leer los datos de los archivos Excel y devolverlos en forma de DataFrame de Spark.

### 1.2. Transformación de Datos (Transform)
- **Limpieza de Datos**: Los datos se limpian para eliminar caracteres especiales, estandarizar formatos y manejar valores nulos.
- **Estandarización de Datos**: Se estandarizan los datos para asegurar la consistencia en los formatos de texto y fechas.
- **Clase Transformacion**: Esta clase se encarga de realizar las transformaciones necesarias en los DataFrames, incluyendo la limpieza y estandarización de datos.

### 1.3. Carga de Datos (Load)
- **Destino de Datos**: Los datos transformados se guardan en archivos CSV en un directorio de salida.
- **Clase Carga**: Esta clase se encarga de guardar los DataFrames transformados en archivos CSV.

## 2. Arquetipo de la Aplicación

El arquetipo de la aplicación sigue una estructura modular, donde cada módulo se encarga de una parte específica del proceso ETL. A continuación, se describe la estructura del proyecto y el flujo de trabajo de la aplicación.

### 2.1. Estructura del Proyecto

DATA_ENGINEERING_PROYECT/
├── data/
│ ├── originales/
│ │ └── Films_2.xlsx
│ └── salidas/
├── ETL/
│ ├── extract.py
│ ├── transform.py
│ └── load.py
├── report/
│ └── informe.ipynb
├── main.py
└── ...


### 2.2. Flujo de Trabajo
1. **Extracción**: Los datos se extraen de los archivos Excel ubicados en `data/originales/`.
2. **Transformación**: Los datos se limpian y estandarizan utilizando las funciones definidas en `ETL/transform.py`.
3. **Carga**: Los datos transformados se guardan en archivos CSV en el directorio `data/salidas/`.
4. **Reporte**: Se genera un informe en Jupyter Notebook (`report/informe.ipynb`) para visualizar los resultados del proceso ETL.
5. **Ejecución**: El flujo completo se ejecuta desde `main.py`.