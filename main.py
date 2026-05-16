from pyspark.sql import SparkSession
from pipeline.reader import DataReader
from pipeline.transformer import DataTransformer
from pipeline.writer import DataWriter

def run_pipeline():
    # 1. Encendemos el motor de Spark
    spark = SparkSession.builder \
        .appName("BancoModularEngine") \
        .master("local[*]") \
        .getOrCreate()
        
    print("🚀 [ENGINE] Motor Spark inicializado de forma modular.")

    # Inicializamos nuestros componentes
    reader = DataReader(spark)
    transformer = DataTransformer()
    writer = DataWriter()

    # 2. Corremos el flujo (Pipeline)
    df_bronce = reader.read_csv("datos_bronce_banco.csv")
    df_silver = transformer.clean_transactions(df_bronce)
    writer.write_parquet(df_silver, "capa_plata_spark/transacciones_mexico")

    print("🏁 [ENGINE] Proceso terminado exitosamente.")
    spark.stop()

if __name__ == "__main__":
    run_pipeline()