from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

numero = 1
# Definindo alguns argumentos básicos
default_args = {
   'owner': 'felipe',
   'depends_on_past': False,
   'start_date': datetime(2022, 1, 1),
   'retries': 0,
   }
# Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias as 8 da manhã)
with DAG(
   'dagteste',
   schedule_interval=timedelta(seconds=15),
   catchup=False,
   default_args=default_args
   ) as dag:
# Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
    t1 = BashOperator(
    task_id='primeiroscript',
    bash_command="""
    cd /home/felipe/Documentos/pibic/testes
    python3 leituracsv.py
    """)
    t2 = BashOperator(
    task_id='secundoscript',
    bash_command="""
    cd /home/felipe/Documentos/pibic/testes
    python3 dbinput.py
    """)
    t3 = BashOperator(
    task_id='terceiroscript',
    bash_command="""f
    cd /home/felipe/Documentos/pibic/testes
    python3 sqlread.py {numero}
    """)
    # Definindo o padrão de execução, nesse caso executamos t1 e depois t2
    t1 >> t2 >> t3