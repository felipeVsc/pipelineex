from geraView import *

with open('query.txt','r') as file:
    dados = file.read()

spark,view = retornarView()

sqlres = spark.sql(dados)
sqlres.show()