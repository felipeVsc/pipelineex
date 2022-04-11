from pyspark.sql import SparkSession

def retornarView():

    spark = SparkSession \
        .builder \
        .appName("Exemplo Simples") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()


    dadosCsv = spark.read.option("header",True).csv('/home/felipe/Documentos/datasets/brasil/states.csv')

    return spark,dadosCsv.createOrReplaceTempView("data")