class DataWriter:
    def __init__(self):
        pass

    def write_parquet(self, df, output_path):
        print(f"✅ [WRITER] Escribiendo archivos Parquet optimizados en: {output_path}")
        df.write \
            .mode("overwrite") \
            .parquet(output_path)