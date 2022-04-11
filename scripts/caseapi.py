from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession

def transformJson(**kwargs):
    
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='get_op')
    print("ls :",type(ls))

    file = open('/home/felipe/airflow/dags/scripts/dados.json','w')
    file.write(ls)
    file.close()
    # with open('dados.json','w') as file:
    #     file.write(ls)

    return "Dados salvos"

def SparkFuncao():
    spark = SparkSession \
        .builder \
        .appName("Exemplo Simples") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()


    dadosJson = spark.read.json('/home/felipe/airflow/dags/scripts/dados.json')
    dadosJson.show()

    dadosJson.createOrReplaceTempView("data")

    spark.sql('select bitcoin from data').show()

    
# Definindo alguns argumentos básicos
default_args = {
   'owner': 'felipe',
   'depends_on_past': False,
   'start_date': datetime(2022, 1, 1),
   'retries': 0,

}
# Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias as 8 da manhã)
with DAG(
   'caseapi',
   schedule_interval=timedelta(seconds=60),
   catchup=False,
   default_args=default_args
   ) as dag:
# Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
    
    taskGetAPI = SimpleHttpOperator(
    task_id='get_op',
    method='GET',
    http_conn_id='dados_api',
    endpoint='/simple/price',
    data={"ids":"bitcoin,ethereum,dogecoin,litecoin,tether","vs_currencies":"brl"},
    headers={"X-RapidAPI-Host": "coingecko.p.rapidapi.com",
	"X-RapidAPI-Key": "8598cb90aemsh6e8685d090d4329p1db6a8jsna6e56e2678b6"},
    dag=dag
    )

    taskJson = PythonOperator(
    # value = taskGetAPI.xcom_pull(task_ids='get_op'),
    task_id='transformaEmJson',
    python_callable=transformJson,
    provide_context = True,
    dag = dag,    
)
    taskSpark = PythonOperator(
    task_id='sparkscript',
    python_callable=SparkFuncao,
    provide_context = True,
    dag=dag
    )
    # Definindo o padrão de execução, nesse caso executamos t1 e depois t2
    taskGetAPI >> taskJson >> taskSpark