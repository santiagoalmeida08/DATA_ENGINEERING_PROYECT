"""Testing unitario de clases"""

#Extraccion

import os
from ETL.extract import Extraccion
from pyspark.sql import SparkSession

os.environ['JAVA_HOME'] = 'C:\\java_final\\jdk-11'
os.environ['SPARK_HOME'] = 'C:\\spark\\spark-3.5.4-bin-hadoop3'

file_path = 'C:\\Users\\Usuario\\Desktop\\Pruebas_tecnicas\\Prueba_quid\\DATA_ENGINEERING_PROYECT\\data\\Films_2.xlsx'
#Iniciar

spark = SparkSession.builder.master('local[*]').appName('ELT').getOrCreate()

