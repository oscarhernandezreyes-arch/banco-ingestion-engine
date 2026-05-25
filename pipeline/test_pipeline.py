import pytest
from pyspark.sql import SparkSession
# Importamos la clase desde tu archivo transformer
from pipeline.transformer import DataTransformer 

@pytest.fixture(scope="session")
def spark():
    # Levanta el entorno Spark local de forma limpia
    return SparkSession.builder \
        .master("local[*]") \
        .appName("Pruebas-Banco-Engine") \
        .getOrCreate()

def test_clean_transactions(spark):
    # 1. Instanciamos tu clase transformer
    transformer = DataTransformer()
    
    # 2. Creamos datos de juguete con un monto negativo y un cliente millonario (VIP)
    datos = [
        ("Juan", 1000.0, "MX"),
        ("Pedro", -50.0, "MX"),  # Debería borrarse por ser <= 0
        ("Carlos", 600000.0, "US") # Debería volverse VIP
    ]
    columnas = ["cliente", "monto", "pais"]
    df_prueba = spark.createDataFrame(datos, columnas)
    
    # 3. Corremos tu función de limpieza
    df_resultado = transformer.clean_transactions(df_prueba)
    
    # 4. EXÁMENES (Aserciones):
    # - El registro negativo se debió eliminar (quedan 2 registros)
    assert df_resultado.count() == 2
    
    # - El cliente Carlos debe estar oculto como VIP
    vip_user = df_resultado.filter(df_resultado.monto == 600000.0).collect()[0]
    assert vip_user["cliente_anonimo"] == "USUARIO_VIP_OCULTO"

def test_aggregate_by_country(spark):
    transformer = DataTransformer()
    
    # Datos para probar el Group By
    datos = [
        (500.0, "MX"),
        (1500.0, "MX"),
        (2000.0, "US")
    ]
    columnas = ["monto", "pais"]
    df_prueba = spark.createDataFrame(datos, columnas)
    
    # Corremos la agregación
    df_resultado = transformer.aggregate_by_country(df_prueba)
    
    # EXAMEN: El monto total de MX debería ser 2000.0 (500 + 1500)
    monto_mx = df_resultado.filter(df_resultado.pais == "MX").collect()[0]["monto_total_pais"]
    assert monto_mx == 2000.0