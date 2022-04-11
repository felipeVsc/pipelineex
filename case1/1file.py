from urllib import response
from pyspark.sql import SparkSession
import requests
import json

# spark = SparkSession \
#     .builder \
#     .appName("Exemplo Simples") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()


url = "https://coingecko.p.rapidapi.com"

query = {"ids":"bitcoin,ethereum","vs_currencies":"brl"}

headers = {
	"X-RapidAPI-Host": "coingecko.p.rapidapi.com",
	"X-RapidAPI-Key": "8598cb90aemsh6e8685d090d4329p1db6a8jsna6e56e2678b6"
}

def getCurrentPrice(lista):
    urlNovo = url+"/simple/price"
    stringIds = ','.join(lista)
    query = {"ids":stringIds,"vs_currencies":"brl"}
    return requests.request("GET", urlNovo, headers=headers,params=query).json()


def getMarketChartLastWeek(idCripto):
    urlNovo = url+"/coins/"+idCripto+"/market_chart"
    
    query = {"vs_currency":"brl","days":"7","interval":"daily"}
    return requests.request("GET", urlNovo, headers=headers,params=query).json()


response = getMarketChartLastWeek('bitcoin')

print(len(response['prices']))
# dados = json.loads(dados)

# print(dados)







# Ler aqui o arquivo CSV e fazer aqui as operações
# Ver como utiliza o Airflow para fazer o GET da API
# Fazer também por aqui
# Passar resultados para os arquivos