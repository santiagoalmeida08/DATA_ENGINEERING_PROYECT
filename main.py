from ETL.extract import Extraccion
from ETL.transform import Transformacion
from ETL.load import Carga

def main():
    input_path = 'C:\\Users\\Usuario\\Desktop\\Pruebas_tecnicas\\Prueba_quid\\DATA_ENGINEERING_PROYECT\\data\\originales\\Films_2.xlsx'
    output_path = 'C:\\Users\\Usuario\\Desktop\\Pruebas_tecnicas\\Prueba_quid\\DATA_ENGINEERING_PROYECT\\data\\salidas'

    hojas = ['film', 'inventory', 'retail', 'costumer', 'store']
    extractor = Extraccion(input_path)
    cargador = Carga(output_path)

    for hoja in hojas:
        print(f"Procesando hoja: {hoja}")
        df = extractor.extraccion_datos(hoja)
        transformador = Transformacion(df)
        transformador.analizar_datos()
        df_limpio = transformador.limpiar_datos()
        cargador.guardar_datos(df_limpio, f"{hoja}_limpio.csv")

if __name__ == "__main__":
    main()



