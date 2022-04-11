import pandas as pd
import matplotlib.pyplot as plt
from geraView import *

spark,view = retornarView()

populationQuery = spark.sql("select * from data where data.Population>10000000")
pdDf = populationQuery.toPandas()


gdpQuery = spark.sql("select data.GDP, data.State from data")
# gdpQuery.show()
gdpPd = gdpQuery.toPandas()


areaQuery = spark.sql("select data.Area, data.State from data")
# areaQuery.show()
areaPd = areaQuery.toPandas()

dicionario = areaPd.to_dict("records")
dicionarioCorreto = {x['State']:float(x['Area']) for x in dicionario}
ordenado = [(key, value) for (key, value) in sorted(dicionarioCorreto.items(), key=lambda x: x[1],reverse=True)]

qntCidades = spark.sql("select `Cities count`, data.State from data")
# qntCidades.show()
cidadesPd = gdpQuery.toPandas()

# gdp
gdp = pd.to_numeric(pdDf['GDP'])
plot1 = plt.bar(pdDf['State'],gdp) 

plt.savefig("stategdp.png")

pop = pd.to_numeric(pdDf['Population'])

plot = plt.bar(pdDf['State'],pop)

plt.savefig("statepop.png") # arrumar isso aqui

# # pie
lista = [(x[1]/8516000)*100 for x in ordenado]
listaNomes = [x[0] for x in ordenado]
plot2 = plt.pie(lista,labels=listaNomes)
plt.savefig("statepie.png")
# plt.show()

