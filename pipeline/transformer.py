from pyspark.sql.functions import col, when

class DataTransformer:
    def __init__(self):
        pass

    def clean_transactions(self, df):
        print("⚡ [TRANSFORMER] Aplicando reglas de negocio y Data Masking VIP...")
        return df \
            .filter(col("pais") == "Mexico") \
            .withColumn("cliente_anonimo", 
                        when(col("monto") > 500000.0, "USUARIO_VIP_OCULTO")
                        .otherwise(col("cliente"))) \
            .drop("cliente")