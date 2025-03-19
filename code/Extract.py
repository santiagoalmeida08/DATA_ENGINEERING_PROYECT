""" En este Sccript se realiza un analisis exploratorio de las bases proporcionadas para detectar anomalias """

# Librerias
import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import pandas as pd 
import openpyxl 

# Establecer variables de entorno y ruta de acceso

os.environ['JAVA_HOME'] = 'C:\\java_final\\jdk-11'
os.environ['SPARK_HOME'] = 'C:\\spark\\spark-3.5.4-bin-hadoop3'

path = 'C:\\Users\\Usuario\\Desktop\\Pruebas_tecnicas\\Prueba_quid\\DATA_ENGINEERING_PROYECT\\data'

# Inicializar sesion de spark
findspark.init()
#spark = SparkSession.builder.appName('ETL_films').getOrCreate()

# Extraemos los datos 

class Extraccion:
    
    def __init__(self, file_path):
        self.file_path = file_path
        self.spark = SparkSession.builder \
                    .master('local[*]') \
                    .config('spark.jars.packages', 'com.crealytics:spark-excel_2.13-3.5.0_0.20.3')\
                    .appName('ELT')\
                    .getOrCreate()
    
    def carga_datos(self):
        
        """Metodo para leer los datos de un excel
        """
        try : 
             
            df_films = self.spark.read \
                .format('com.crealytics.spark.excel') \
                .option('inferSchema','true') \
                .option('dataAddress', 'film') \
                .load(self.file_path)
            
            return df_films
        
        except Exception as e:
            raise Exception(f'Error al extraer datos {e}')
    
df = Extraccion(path + '\\Films_2.xlsx')
df.show(10, truncate = False)