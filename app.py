import requests
import pandas as pd
from datetime import datetime, date, timedelta
import os
import sqlalchemy as sa
from sqlalchemy import create_engine, Date, text
#from config import *


# Leer variables de entorno
HOST = os.environ.get("HOST")
PORT = os.environ.get("PORT")
USER = os.environ.get("USER")
PWD = os.environ.get("PASSWORD")
DB = os.environ.get("DB")
SCHEMA = os.environ.get("SCHEMA")

# Función que convierte un Json en un DataFrame
def build_table(json_data):
    df = pd.json_normalize(json_data)
    return df

def proceso_agregar_columnas(df):
     # Convierte la columna 'timestamp_measured' en datetime si es de tipo objeto
    df['fechaActualizacion'] = pd.to_datetime(df['fechaActualizacion'])
    # Ahora puedes realizar las operaciones con las fechas
    df["fecha"] = df['fechaActualizacion'].dt.date
    df["hora"] = df['fechaActualizacion'].dt.hour

    return df

# Función que obtiene el dato y retorna una DataFrame
def get_data(base_url, endpoint, params=None):
   
   try:  
       # Apuntamos a la url del endpoit, hacemos la petición de tipo GET 
       # y la guardamos el objeto de tipo Response en la variable response
         endpoint_url = f"{base_url}/{endpoint}"
         response = requests.get(endpoint_url, params=params)
         response.raise_for_status() 
         
       #  LLamamos a la funcion def build_table(data) para convertir el dato en DataFrame y luego agregamos columnas 
         data=response.json()
         df_data= build_table(data)
         df_data= proceso_agregar_columnas(df_data)
         
         return df_data
     
   except requests.exceptions.RequestException as e:
        print(f"La petición ha fallado. Código de error : {e}")
        return None 
    

def save(df, save_path, partition_cols=None, engine="fastparquet"):
    # Crear el directorio si no existe
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    df.to_parquet(
        save_path,
        partition_cols=partition_cols,
        engine=engine
    )
    
    

def connect_to_postgres():
    """
    Establece una conexión a una base de datos postgres
    utilizando la configuración especificada en un archivo config.py.

    
    Returns:
        sqlalchemy.engine.Engine: La conexión a la base de datos de postgres.
    """
    # Leer la configuración desde el archivo INI
 
           # Establecer la conexión a la base de datos PostgreSQL
    url = f"postgresql://{USER}:{PWD}@{HOST}:{PORT}/{DB}?sslmode=require"
    conn = sa.create_engine(url,
                              connect_args={"options": f"-c search_path={SCHEMA}"}
                              )
    return conn

  
  
  # URL base y tres endpoints.
base_url = "https://dolarapi.com"
endpoint_dolar_oficial = "/v1/dolares/oficial" # Técnica extracción full
endpoint_dolar_blue = "/v1/dolares/blue" # Técnica extracción incremental 
endpoint_dolares = "/v1/dolares" # Técnica extracción full

# #DataFrame de todos los tipos de cambio  
df_dolares= get_data(base_url, endpoint_dolares)
df_dolar_oficial= get_data(base_url, endpoint_dolar_oficial)
df_dolar_blue= get_data(base_url, endpoint_dolar_blue)


dfs = [df_dolares, df_dolar_oficial, df_dolar_blue]
paths = ["tipos_cambio_dolar", "dolar_oficial", "dolar_blue"]
partition_cols_list = ["fecha", "fecha", "fecha"]

# Itera sobre los endpoints, nombres de archivo y columnas de partición para obtener y guardar los datos en la capa bronze
for df, path, partition_cols in zip(dfs, paths, partition_cols_list):
      save(df, f'datalake/bronze/dolares/{path}', partition_cols, engine="fastparquet")


def filtrar_convertir_limpiar(df):
    columnas_deseadas = ['casa', 'moneda', 'compra', 'venta', 'fecha', 'hora']
    
    # Hacer una copia del DataFrame para evitar SettingWithCopyWarning
    df_filtrado = df[columnas_deseadas].copy()
   
    # Reemplazar NaN por 0.00 en las columnas 'compra' y 'venta'
    df_filtrado['compra'].fillna(0.00, inplace=True)
    df_filtrado['venta'].fillna(0.00, inplace=True)
    
   
    df_filtrado['fecha'] = pd.to_datetime(df_filtrado['fecha'])
    df_filtrado['casa'] = df_filtrado['casa'].astype('category')
    
     # Agregar una columna 'id' con valores únicos para cada registro
    df_filtrado['id'] = range(1, len(df_filtrado) + 1)
     # Reorganizar columnas para colocar 'id' al principio
    column_order = ['id'] + columnas_deseadas
    df_filtrado = df_filtrado[column_order]
    
    return df_filtrado

# Se leen los arqchivos parquet bronze
tipos_cambio_dolar_parquet = pd.read_parquet('datalake/bronze/dolares/tipos_cambio_dolar') 
dolar_oficial_parquet = pd.read_parquet('datalake/bronze/dolares/dolar_oficial')
dolar_blue_parquet = pd.read_parquet('datalake/bronze/dolares/dolar_blue') 

# Directorios y nombres de archivo
directorios = ['dolar_oficial', 'dolar_blue', 'tipos_cambio_dolar']
dfs_parquet = [dolar_oficial_parquet, dolar_blue_parquet, tipos_cambio_dolar_parquet]

# Iterar sobre los DataFrames, nombres de archivo y  guarda el dataframe en la capa silver
for df, path, tail_rows in zip(dfs_parquet, directorios, [1, 1, 7]):
    df_resultado = filtrar_convertir_limpiar(df)
    df_resultado = df_resultado.tail(tail_rows)
    save(df_resultado, f'datalake/silver/dolares/{path}', partition_cols=None, engine="fastparquet")
    
# Crear en postgres tabla dolares SCD Tipo 1

# Crear en postgres tabla dolares SCD Tipo 1

eng = connect_to_postgres()
with eng.begin() as con:
    con.execute(text("""
        CREATE TABLE IF NOT EXISTS leomarestrada_stg_dolares (
            id INT PRIMARY KEY,
            casa VARCHAR(50),
            moneda VARCHAR(50),
            compra DOUBLE PRECISION,
            venta DOUBLE PRECISION,
            fecha DATE,
            hora INTEGER
        );
    """))
    
    con.execute(text("""
        CREATE TABLE IF NOT EXISTS leomarestrada_dolares_scd1 (
            id INT PRIMARY KEY,
            casa VARCHAR(50),
            moneda VARCHAR(50),
            compra DOUBLE PRECISION,
            venta DOUBLE PRECISION,
            fecha DATE,
            hora INTEGER,
            fecha_actualizacion_dwh TIMESTAMP
        );
    """))
    

tipos_cambio_dolar_parquet_silver = pd.read_parquet('datalake/silver/dolares/tipos_cambio_dolar') 

with eng.begin() as con:
    con.execute(text("TRUNCATE TABLE leomarestrada_stg_dolares"))

    tipos_cambio_dolar_parquet_silver.to_sql("leomarestrada_stg_dolares", con,
                        if_exists="append", method="multi",
                        index=False)

    con.execute(text("""
        MERGE INTO leomarestrada_dolares_scd1
        USING leomarestrada_stg_dolares AS dolares
        ON (dolares.id = leomarestrada_dolares_scd1.id)
        WHEN MATCHED THEN
            UPDATE SET
                casa = dolares.casa,
                moneda = dolares.moneda,
                compra = dolares.compra,
                venta = dolares.venta,
                fecha= dolares.fecha,
                hora= dolares.hora,
                fecha_actualizacion_dwh = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (id, casa, moneda, compra, venta, fecha, hora, fecha_actualizacion_dwh)
            VALUES (
                dolares.id,
                dolares.casa,
                dolares.moneda,
                dolares.compra,
                dolares.venta,
                dolares.fecha,
                dolares.hora,
                CURRENT_TIMESTAMP
            );
    """))
    
    
    
with eng.begin() as con:
    con.execute(text("""
        CREATE TABLE IF NOT EXISTS leomarestrada_dolar_blue_scd2 (
            id_scd2 SERIAL PRIMARY KEY,
            id INT,
            casa VARCHAR(50),
            moneda VARCHAR(50),
            compra DOUBLE PRECISION,
            venta DOUBLE PRECISION,
            fecha_inicio DATE,
            fecha_fin DATE,
            es_actual BOOLEAN
        )
    """))
    
with eng.begin() as con:
    con.execute(text("""
        CREATE TABLE IF NOT EXISTS leomarestrada_dolar_oficial_scd2 (
            id_scd2 SERIAL PRIMARY KEY,
            id INT,
            casa VARCHAR(50),
            moneda VARCHAR(50),
            compra DOUBLE PRECISION,
            venta DOUBLE PRECISION,
            fecha_inicio DATE,
            fecha_fin DATE,
            es_actual BOOLEAN
        )
    """))
    
dolar_blue_silver = pd.read_parquet('datalake/silver/dolares/dolar_blue') 

with eng.begin() as con:
    con.execute(text("TRUNCATE TABLE leomarestrada_stg_dolares"))

    dolar_blue_silver.to_sql("leomarestrada_stg_dolares", con,
                              if_exists="append", method="multi",
                              index=False)

    con.execute(text("""
       BEGIN;

       MERGE INTO leomarestrada_dolar_blue_scd2
       USING leomarestrada_stg_dolares AS dolares
       ON (dolares.id = leomarestrada_dolar_blue_scd2.id)
       WHEN MATCHED AND dolares.venta <> leomarestrada_dolar_blue_scd2.venta AND leomarestrada_dolar_blue_scd2.es_actual = TRUE THEN
            UPDATE SET
                fecha_fin = CURRENT_DATE,
                es_actual = FALSE
       WHEN NOT MATCHED THEN
            INSERT (id, casa, moneda, compra, venta, fecha_inicio, fecha_fin, es_actual)
            VALUES (
                dolares.id,
                dolares.casa,
                dolares.moneda,
                dolares.compra,
                dolares.venta,
                dolares.fecha,
                NULL,
                TRUE
            );

        INSERT INTO leomarestrada_dolar_blue_scd2 (id, casa, moneda, compra, venta, fecha_inicio, es_actual)
        SELECT s.id, s.casa, s.moneda, s.compra, s.venta, CURRENT_DATE, TRUE
        FROM leomarestrada_stg_dolares s
        LEFT JOIN leomarestrada_dolar_blue_scd2 c
        ON s.id = c.id
        WHERE s.venta <> c.venta AND c.es_actual = FALSE;

        COMMIT;
    """))
    
    
dolar_blue_silver = pd.read_parquet('datalake/silver/dolares/dolar_oficial') 

with eng.begin() as con:
    con.execute(text("TRUNCATE TABLE leomarestrada_stg_dolares"))

    dolar_blue_silver.to_sql("leomarestrada_stg_dolares", con,
                              if_exists="append", method="multi",
                              index=False)

    con.execute(text("""
       BEGIN;

       MERGE INTO leomarestrada_dolar_oficial_scd2
       USING leomarestrada_stg_dolares AS dolares
       ON (dolares.id = leomarestrada_dolar_oficial_scd2.id)
       WHEN MATCHED AND dolares.venta <> leomarestrada_dolar_oficial_scd2.venta AND leomarestrada_dolar_oficial_scd2.es_actual = TRUE THEN
            UPDATE SET
                fecha_fin = CURRENT_DATE,
                es_actual = FALSE
       WHEN NOT MATCHED THEN
            INSERT (id, casa, moneda, compra, venta, fecha_inicio, fecha_fin, es_actual)
            VALUES (
                dolares.id,
                dolares.casa,
                dolares.moneda,
                dolares.compra,
                dolares.venta,
                dolares.fecha,
                NULL,
                TRUE
            );

        INSERT INTO leomarestrada_dolar_oficial_scd2 (id, casa, moneda, compra, venta, fecha_inicio, es_actual)
        SELECT s.id, s.casa, s.moneda, s.compra, s.venta, CURRENT_DATE, TRUE
        FROM leomarestrada_stg_dolares s
        LEFT JOIN leomarestrada_dolar_oficial_scd2 c
        ON s.id = c.id
        WHERE s.venta <> c.venta AND c.es_actual = FALSE;

        COMMIT;
    """))    
    
    
