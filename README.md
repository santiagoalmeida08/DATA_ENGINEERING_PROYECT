# DATA_ENGINEERING_PROYECT

Este proyecto implementa un proceso ETL utilizando Programación Orientada a Objetos (POO). El proceso incluye:

1. **Extracción**: Lectura de hojas de Excel.
2. **Transformación**: Análisis y limpieza de datos (nulos, duplicados, tipos de datos, caracteres especiales).
3. **Carga**: Guardado de datos limpios en formato CSV en la carpeta `data/salidas`.

## Requisitos previos

Antes de ejecutar el proyecto, asegúrate de cumplir con los siguientes requisitos:

1. **Instalar Java y Apache Spark**:
   - Descarga e instala Java Development Kit (JDK) 11.
   - Descarga e instala Apache Spark (versión 3.5.4 o compatible).
   - Configura las variables de entorno:
     - `JAVA_HOME`: Ruta al directorio de instalación de Java.
     - `SPARK_HOME`: Ruta al directorio de instalación de Spark.

2. **Instalar dependencias de Python**:
   - Asegúrate de tener Python instalado (versión 3.8 o superior).
   - Instala las dependencias necesarias ejecutando:
     ```bash
     pip install -r requirements.txt
     ```

3. **Verificar rutas de archivos**:
   - Asegúrate de que el archivo de entrada `Films_2.xlsx` esté ubicado en la carpeta `data/originales`.
   - Verifica que las rutas configuradas en el código coincidan con las rutas de tu sistema.

## Ejecución del proyecto

Sigue estos pasos para ejecutar el proyecto:

1. **Clonar el repositorio**:
   Si aún no lo has hecho, clona el repositorio en tu máquina local:
   ```bash
   git clone <URL_DEL_REPOSITORIO>
   cd DATA_ENGINEERING_PROYECT
   ```

2. **Ejecutar el script principal**:
   Ejecuta el archivo `main.py` desde la terminal o un entorno de desarrollo:
   ```bash
   python main.py
   ```

3. **Progreso del proceso ETL**:
   - El script procesará las hojas de Excel especificadas (`film`, `inventory`, `rental`, `customer`, `store`).
   - Durante la ejecución, se mostrarán mensajes en la consola indicando el progreso de cada etapa (extracción, transformación y carga).

4. **Verificar resultados**:
   - Los archivos procesados se guardarán en la carpeta `data/salidas` en formato CSV.
   - Cada archivo tendrá el nombre `<nombre_hoja>_limpio.csv`.

## Notas adicionales

- Si encuentras errores durante la ejecución, revisa los mensajes de error en la consola para identificar el problema.
- Asegúrate de que las dependencias y las configuraciones del entorno estén correctamente instaladas y configuradas.