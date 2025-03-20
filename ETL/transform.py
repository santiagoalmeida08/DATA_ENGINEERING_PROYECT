# Librerias

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, desc,lower,trim,to_date
import pandas as pd 
import logging 
from extract import Extraccion
from pyspark.sql.functions import regexp_extract, regexp_replace
from pyspark.sql.types import StringType, DateType, IntegerType

# Establecer variables de entorno y ruta de acceso

os.environ['JAVA_HOME'] = 'C:\\java_final\\jdk-11'
os.environ['SPARK_HOME'] = 'C:\\spark\\spark-3.5.4-bin-hadoop3'

path = 'C:\\Users\\Usuario\\Desktop\\Pruebas_tecnicas\\Prueba_quid\\DATA_ENGINEERING_PROYECT\\data\\originales\\Films_2.xlsx'

logger = logging.getLogger(__name__)

ex = Extraccion(path)

#Test_lectura

try: 

    df_films = ex.extraccion_datos('film')
    df_inventory = ex.extraccion_datos('inventory')
    df_retail = ex.extraccion_datos('rental')
    df_customer = ex.extraccion_datos('customer')
    df_store = ex.extraccion_datos('store')
    
    logger.info(f"Extraccion exitosa")
    
except Exception as e:
    logger.error(f"Error al extraer datos: {e}")
    raise

#Testeo
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
    
    def estandarizacion(self):
        
        string_columns = [column for column, dtype in self.df.dtypes if dtype == 'string']
        
        for column in string_columns:
            
            self.df = self.df.withColumn(column, lower(col(column)))
            self.df = self.df.withColumn(column, trim(col(column)) )
        
        return self.df

    def eliminar_columnas_nulas(self):
        
        null_counts = self.df.select([col(c).isNull().cast("int").alias(c) for c in self.df.columns]) \
                                .agg(*[sum(col(c)).alias(c) for c in self.df.columns]).collect()[0]
        
        columns_to_drop = [column for column in self.df.columns if null_counts[column] == self.df.count()]
        
        return self.df.drop(*columns_to_drop)
    
    
    def eliminar_caracteres_especiales(self):
        for column in self.df.columns:
            if isinstance(self.df.schema[column].dataType, StringType):
                self.df = self.df.withColumn(column, 
                                            regexp_replace(col(column), "[^a-zA-Z0-9@._-]", ""))
                
                if "date" in column.lower():
                    self.df = self.df.withColumn(column, 
                                                regexp_replace(col(column), "[^0-9-]", "")) \
                                    .withColumn(column, to_date(col(column), "yyyy-MM-dd"))
                    
        return self.df
    
    def transform(self):
        
        self.df = self.estandarizacion()
        self.df = self.eliminar_columnas_nulas()
        self.df = self.eliminar_caracteres_especiales()
    
        return self.df
        
    

d_1 = Transformacion(df_films).analizar_datos()



