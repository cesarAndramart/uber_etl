import gcsfs
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from google.cloud import bigquery
from pyspark.sql import Row
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import functions as F
import gcsfs
import pandas as pd


spark = SparkSession.builder \
    .appName("MiScriptDeSpark") \
    .getOrCreate()


def get_last_id(table_name, id_name):
    try:
        client = bigquery.Client()

        # Especifica el ID de la tabla en formato: project.dataset.table
        table_id = f"psychic-setup-442604-n4.uber_practice.{table_name}"

        # Ejecuta la consulta SQL o lee la tabla directamente
        query = f"SELECT MAX({id_name}) as max_id FROM `{table_id}` AS max_id"
        query_job = client.query(query)

        result = query_job.result()  # Esto devuelve un iterador
        return int(next(result)[0])
    except:
        return None

def read_dimension_to_pandas(table_name):
    client = bigquery.Client()

    table_id = f"psychic-setup-442604-n4.uber_practice.{table_name}"

    # Usa list_rows para obtener todas las filas de la tabla
    rows = client.list_rows(table_id)

    # Convierte las filas en un DataFrame de Pandas
    df = rows.to_dataframe()
    return df

def upload_to_bigquery(dataframe, table_name):
# Define el ID del proyecto y del dataset
    project_id = "project_id"
    dataset_id = "dataset_id"

    table_id = table_name

    pandas_dataframe = dataframe.toPandas()

    client = bigquery.Client()

    table_ref = client.dataset(dataset_id).table(table_id)

    # Cargar el DataFrame de Pandas a BigQuery
    job = client.load_table_from_dataframe(pandas_dataframe, table_ref)

    job.result()

    return True
#Create dimension for the case when it is the first time we create the table
def create_dimension(df, dim_name):
    if dim_name == 'rate':
        dim_name = 'RatecodeID'
    dim_df = df.select(dim_name)
    dim_df = dim_df.distinct()
    dim_df = dim_df.filter(dim_df[dim_name].isNotNull())
    window_spec = Window.orderBy(dim_name)
    df_final = dim_df.withColumn(f'{dim_name}_id', row_number().over(window_spec))
    if dim_name == 'payment_type':
        payment_type_dict = {1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"}
        condition = F.lit("Other")

        for key, value in payment_type_dict.items():
            condition = F.when(df_final["payment_type"] == key, value).otherwise(condition)

        # Agregar la nueva columna basada en el diccionario
        df_final = df_final.withColumn("payment_type_name", condition)
    elif dim_name == 'RatecodeID':
        rate_code_dict = {1:'Standard rate',
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"}
        condition = F.lit("Other")
        for key, value in rate_code_dict.items():
            condition = F.when(df_final["RatecodeID"] == key, value).otherwise(condition)
        df_final = df_final.withColumn("rate_code_name", condition)
    upload_to_bigquery(df_final, f'{dim_name}_dim')

    return True
#Update dimension when we table is already created.
def update_dimension(df, dim_name):
    try:
        current_dim = read_dimension_to_pandas(f'{dim_name}_dim')
        current_dim = current_dim[[current_dim.columns[0]]]
        if dim_name == 'rate':
            dim_name = 'RatecodeID'
        dim_df = df.select(dim_name)
        dim_df = dim_df.distinct()
        dim_df = dim_df.filter(df[dim_name].isNotNull())
        dim_df = dim_df.select('*').toPandas()
        print(dim_df.head())
        df_combined = pd.concat([dim_df, current_dim]).drop_duplicates().reset_index(drop=True)
        df_combined = df_combined.sort_values(by=[df_combined.columns[0]])
        df_final = spark.createDataFrame(df_combined)
        window_spec = Window.orderBy(dim_name)
        df_final = df_final.withColumn(f'{dim_name}_id', row_number().over(window_spec))
        if dim_name == 'payment_type':
            payment_type_dict = {1:"Credit card",
            2:"Cash",
            3:"No charge",
            4:"Dispute",
            5:"Unknown",
            6:"Voided trip"}
            condition = F.lit("Other")

            for key, value in payment_type_dict.items():
                condition = F.when(df_final["payment_type"] == key, value).otherwise(condition)

            df_final = df_final.withColumn("payment_type_name", condition)
        elif dim_name == 'RatecodeID':
            rate_code_dict = {1:'Standard rate',
            2:"JFK",
            3:"Newark",
            4:"Nassau or Westchester",
            5:"Negotiated fare",
            6:"Group ride"}
            condition = F.lit("Other")

            for key, value in rate_code_dict.items():
                condition = F.when(df_final["RatecodeID"] == key, value).otherwise(condition)

            df_final = df_final.withColumn("rate_code_name", condition)
        upload_to_bigquery(df_final, f'{dim_name}_dim')
    except:
        create_dimension(df, dim_name)

        
    return True

#Special case when we have date data type in the columns. This is to build the date dimension
def update_date_dimension(df):
    last_id = get_last_id('datetime_dim','datetime_id')
    datetime_df = df.select('tpep_pickup_datetime', 'tpep_dropoff_datetime')
    datetime_df = datetime_df.distinct()
    datetime_df = datetime_df.filter(df["tpep_pickup_datetime"].isNotNull())
    last_id = last_id if last_id is not None else 0  # Si la tabla está vacía, comienza desde 0
    window_spec = Window.orderBy(F.lit(1))
    datetime_df = datetime_df.withColumn('datetime_id', row_number().over(window_spec) + last_id)
    datetime_df = datetime_df.withColumn("pick_year", F.year("tpep_pickup_datetime")) \
                           .withColumn("pick_month", F.month("tpep_pickup_datetime")) \
                           .withColumn("pick_day", F.dayofmonth("tpep_pickup_datetime")) \
                           .withColumn("pick_hour", F.hour("tpep_pickup_datetime")) \
                           .withColumn("pick_weekday", F.dayofweek("tpep_pickup_datetime")) \
                           .withColumn("drop_year", F.year("tpep_dropoff_datetime")) \
                           .withColumn("drop_month", F.month("tpep_dropoff_datetime")) \
                           .withColumn("drop_day", F.dayofmonth("tpep_dropoff_datetime")) \
                           .withColumn("drop_hour", F.hour("tpep_dropoff_datetime")) \
                           .withColumn("drop_weekday", F.dayofweek("tpep_dropoff_datetime"))
    upload_to_bigquery(datetime_df, 'datetime_dim')
    return True

def upload_batch_to_bigquery(batch_df, table_name):
    project_id = "psychic-setup-442604-n4"
    dataset_id = "uber_practice"
    table_id = f"{dataset_id}.{table_name}"

    pandas_dataframe = batch_df.toPandas()
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_name)

    job = client.load_table_from_dataframe(
        pandas_dataframe, 
        table_ref, 
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"Lote subido exitosamente a la tabla {table_id}.")
    return True
#We upload the fact table in 500,000 rows-batch
def update_fact_table(df):
    last_id = get_last_id('fact_table','tripID')
    last_id = last_id if last_id is not None else 0  # Si la tabla está vacía, comienza desde 0

    window_spec = Window.orderBy(F.lit(1))
    df = df.withColumn('tripID', row_number().over(window_spec) + last_id)
    df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
       .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))

# Agregar índices correctamente
    df_with_index = df.rdd.zipWithIndex().map(
        lambda x: Row(**x[0].asDict(), index=x[1])
    ).toDF()

    # Procesar en lotes de 500,000 filas
    batch_size = 500_000
    total_rows = df_with_index.count()
    num_batches = (total_rows // batch_size) + 1

    for i in range(num_batches):
        batch_df = df_with_index.filter(
            (col("index") >= i * batch_size) & (col("index") < (i + 1) * batch_size)
        ).drop("index")  # Elimina la columna de índice si no es necesaria

        # Convertir a Pandas y subir a BigQuery
        upload_batch_to_bigquery(batch_df, "fact_table")
    return True

fs = gcsfs.GCSFileSystem()
file_path = "gs://uber_data_practice_27/raw_data/yellow_tripdata_2024-02.parquet"
df = spark.read.parquet(file_path)

update_dimension(df, 'rate')
update_dimension(df, 'trip_distance')
update_dimension(df, 'passenger_count')
update_dimension(df, 'payment_type')
update_date_dimension(df)
update_fact_table(df)