# Librerias
import os
import findspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, desc
import pandas as pd 
import openpyxl 

from extract import Extraccion

# Establecer variables de entorno y ruta de acceso

os.environ['JAVA_HOME'] = 'C:\\java_final\\jdk-11'
os.environ['SPARK_HOME'] = 'C:\\spark\\spark-3.5.4-bin-hadoop3'

path = 'C:\\Users\\Usuario\\Desktop\\Pruebas_tecnicas\\Prueba_quid\\DATA_ENGINEERING_PROYECT\\data\\originales\\Films_2.xlsx'

ex = Extraccion(path)

#Test

df_films = ex.extraccion_datos('film')
df_inventory = ex(path).extraccion_datos('inventory')
df_retail = ex(path).extraccion_datos('retail')
df_retail = ex(path).extraccion_datos('costumer')
df_retail = ex(path).extraccion_datos('store')

df_films.printSchema()

class Transformacion:
    def __init__(self, df: DataFrame):
        self.df = df

    def analizar_datos(self):
        """Analiza datos nulos, duplicados y tipos de datos."""
        print("Esquema del DataFrame:")
        self.df.printSchema()
        print(f"Cantidad de filas: {self.df.count()}")
        print(f"Cantidad de duplicados: {self.df.dropDuplicates().count()}")
        print("Cantidad de nulos por columna:")
        self.df.select([col(c).isNull().alias(c) for c in self.df.columns]).show()

    def limpiar_datos(self) -> DataFrame:
        """Limpia datos eliminando duplicados y nulos."""
        df_limpio = self.df.dropDuplicates()
        for column in df_limpio.columns:
            df_limpio = df_limpio.filter(col(column).isNotNull())
        return df_limpio



