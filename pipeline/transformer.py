from pyspark.sql.functions import col, when, sum

class DataTransformer:
    def __init__(self):
        pass

    def clean_transactions(self, df):
        print("⚡ [TRANSFORMER] (Narrow) Aplicando filtros y Data Masking VIP...")
        return df \
            .filter(col("monto") > 0) \
            .withColumn("cliente_anonimo", 
                        when(col("monto") > 500000.0, "USUARIO_VIP_OCULTO")
                        .otherwise(col("cliente"))) \
            .drop("cliente")

    def aggregate_by_country(self, df):
        print("🌀 [TRANSFORMER] (Wide) Ejecutando un GROUP BY (Provocando un Shuffle optimizado)...")
        return df \
            .groupBy("pais") \
            .agg(sum("monto").alias("monto_total_pais"))