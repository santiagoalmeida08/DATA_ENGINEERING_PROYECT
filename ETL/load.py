import pandas as pd
import os
from pyspark.sql import SparkSession, DataFrame

list = pd.ExcelFile('C:\\Users\\Usuario\\Desktop\\Pruebas_tecnicas\\Prueba_quid\\DATA_ENGINEERING_PROYECT\\data\\originales\\Films_2.xlsx')
list.sheet_names

class Carga:
    def __init__(self, output_path: str):
        self.output_path = output_path

    def guardar_datos(self, df: DataFrame, file_name: str):
        """Guarda el DataFrame en formato CSV."""
        try:
            output_file = os.path.join(self.output_path, file_name)
            df.write.csv(output_file, header=True, mode='overwrite')
            print(f"Archivo guardado en: {output_file}")
        except Exception as e:
            print(f"Error al guardar datos: {e}")