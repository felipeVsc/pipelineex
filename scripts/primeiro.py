from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

# Definindo alguns argumentos básicos
default_args = {
   'owner': 'felipe',
   'depends_on_past': False,
   'start_date': datetime(2022, 1, 1),
   'retries': 0,
   }
# Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias as 8 da manhã)
with DAG(
   'meuprimeirodag',
   schedule_interval=timedelta(minutes=1),
   catchup=False,
   default_args=default_args
   ) as dag:
# Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
    t1 = BashOperator(
    task_id='primeiroscript',
    bash_command="""
    cd /home/felipe/Documentos/pibic/testes
    python3 testeair.py
    """)
    t2 = BashOperator(
    task_id='secundoscript',
    bash_command="""
    cd /home/felipe/Documentos/pibic/testes
    python3 testeair2.py
    """)
    # Definindo o padrão de execução, nesse caso executamos t1 e depois t2
    t1 >> t2