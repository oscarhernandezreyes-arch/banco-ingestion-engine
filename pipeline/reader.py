class DataReader:
    def __init__(self, spark_session):
        self.spark = spark_session

    def read_csv(self, path):
        print(f"📥 [READER] Leyendo datos nativos desde: {path}")
        return self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(path)