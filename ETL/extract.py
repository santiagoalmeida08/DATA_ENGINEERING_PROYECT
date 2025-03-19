""" En este Sccript se realiza un analisis exploratorio de las bases proporcionadas para detectar anomalias """

# Librerias
import os
import findspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, desc
import pandas as pd 
import openpyxl 
import logging
from typing import Dict, Optional, List, Union

# Establecer variables de entorno y ruta de acceso

os.environ['JAVA_HOME'] = 'C:\\java_final\\jdk-11'
os.environ['SPARK_HOME'] = 'C:\\spark\\spark-3.5.4-bin-hadoop3'

path = 'C:\\Users\\Usuario\\Desktop\\Pruebas_tecnicas\\Prueba_quid\\DATA_ENGINEERING_PROYECT\\data\\originales\\Films_2.xlsx'

# Inicializar sesion de spark
findspark.init()

# Extraemos los datos 

# Configuración de logger
logger = logging.getLogger(__name__)

class Extraccion:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('Extract') \
                    .getOrCreate()

    def extraccion_datos(self, sheet_name: str) -> DataFrame:
        """Método para leer los datos de un Excel."""
        try:
            df = self.spark.read \
                .format('com.crealytics.spark.excel') \
                .option('inferSchema', 'true') \
                .option('header', 'true') \
                .option('dataAddress', f"{sheet_name}!A1") \
                .load(self.file_path)
            return df
        except Exception as e:
            logger.error(f"Error al extraer datos: {e}")
            raise

#Testeando_clase

"""
df_films = Extraccion(path).extraccion_datos('film')
df_inventory = Extraccion(path).extraccion_datos('inventory')
df_retail = Extraccion(path).extraccion_datos('retail')
df_retail = Extraccion(path).extraccion_datos('costumer')
df_retail = Extraccion(path).extraccion_datos('store')


df_films.printSchema()

"""

