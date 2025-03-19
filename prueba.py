import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import pandas as pd 
import openpyxl 

# Establecer variables de entorno y ruta de acceso

os.environ['JAVA_HOME'] = 'C:\\java_final\\jdk-11'
os.environ['SPARK_HOME'] = 'C:\\spark\\spark-3.5.4-bin-hadoop3'

file_path = 'C:\\Users\\Usuario\\Desktop\\Pruebas_tecnicas\\Prueba_quid\\DATA_ENGINEERING_PROYECT\\data\\Films_2.xlsx'
#Iniciar

spark = SparkSession.builder.master('local[*]').appName('ELT').getOrCreate()

#Leer

df_films = spark.read \
    .format('com.crealytics.spark.excel') \
    .option('header', 'true') \
    .option('dataAddress', "'film'!A1") \
    .load(file_path)
    
df_films.show()
    
    
from pyspark.sql import SparkSession

jars_path = 'C:\\spark\\spark-3.5.4-bin-hadoop3\\jars\\spark-excel_2.12-3.5.1_0.20.4.jar'
# Inicializar SparkSession sin necesidad de especificar el JAR 
# (ya que estará disponible en la ruta de Spark)
spark = SparkSession.builder \
    .master("local[*]") \
    .config('spark.jars',jars_path) \
    .appName("ETL") \
    .getOrCreate()

# Ruta del archivo Excel
# file_path = "tu_archivo.xlsx"

# Leer el archivo Excel
df_films = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "films") \
    .load(file_path)

# Mostrar los datos
df_films.show(10, truncate=False)