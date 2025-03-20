from ETL.extract import Extraccion
from ETL.transform import Transformacion
from ETL.load import Carga

def main():
    input_path = 'C:\\Users\\Usuario\\Desktop\\Pruebas_tecnicas\\Prueba_quid\\DATA_ENGINEERING_PROYECT\\data\\originales\\Films_2.xlsx'
    output_path = 'C:\\Users\\Usuario\\Desktop\\Pruebas_tecnicas\\Prueba_quid\\DATA_ENGINEERING_PROYECT\\data\\salidas'

    hojas = ['film', 'inventory', 'rental', 'customer', 'store']
    extractor = Extraccion(input_path)
    cargador = Carga(output_path)

    for hoja in hojas:
        print(f"Procesando hoja: {hoja}")
        try:
            # Extracción
            df = extractor.extraccion_datos(hoja)
            print(f"Datos extraídos correctamente de la hoja: {hoja}")

            # Transformación
            transformador = Transformacion(df)
            transformador.analizar_datos()
            df_transformado = transformador.transform()
            print(f"Datos transformados correctamente para la hoja: {hoja}")

            # Carga
            cargador.guardar_datos(df_transformado, f"{hoja}_limpio.csv")
            print(f"Datos guardados correctamente para la hoja: {hoja}")

        except Exception as e:
            print(f"Error procesando la hoja {hoja}: {e}")

if __name__ == "__main__":
    main()



