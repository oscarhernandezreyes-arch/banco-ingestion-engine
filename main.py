from pyspark.sql import SparkSession
from pipeline.reader import DataReader
from pipeline.transformer import DataTransformer
from pipeline.writer import DataWriter

def run_pipeline():
    spark = SparkSession.builder \
        .appName("BancoAdvancedEngine") \
        .master("local[*]") \
        .getOrCreate()
        
    print("🚀 [ENGINE] Motor Spark encendido para procesamiento avanzado de forma modular.")

    reader = DataReader(spark)
    transformer = DataTransformer()
    writer = DataWriter()

    # 1. READ
    df_bronce = reader.read_csv("datos_bronce_banco.csv")

    # 2. TRANSFORM
    df_silver_clean = transformer.clean_transactions(df_bronce)
    df_gold_metrics = transformer.aggregate_by_country(df_silver_clean)

    print("\n📊 [PREVIEW] Métricas finales consolidadas por País:")
    df_gold_metrics.show()

    # 3. WRITE
    writer.write_parquet(df_gold_metrics, "capa_plata_spark/metricas_paises")

    print("🏁 [ENGINE] Pipeline modular avanzado finalizado con éxito.")

if __name__ == "__main__":
    run_pipeline