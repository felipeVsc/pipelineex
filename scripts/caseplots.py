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
   'dagplots',
   schedule_interval=timedelta(seconds=30),
   catchup=False,
   default_args=default_args
   ) as dag:
# Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
    t1 = BashOperator(
    task_id='p1',
    bash_command="""
    cd /home/felipe/Documentos/pibic/case1
    python3 geraView.py
    """)
    t2 = BashOperator(
    task_id='p2',
    bash_command="""
    cd /home/felipe/Documentos/pibic/case1
    python3 plots.py
    """)
    t3 = BashOperator(
    task_id='p3',
    bash_command="""
    cd /home/felipe/Documentos/pibic/case1
    python3 queries.py
    """)
    # Definindo o padrão de execução, nesse caso executamos t1 e depois t2
    t1 >> t2