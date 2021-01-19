import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sqlalchemy as sal
from sqlalchemy import create_engine

engine = sal.create_engine('postgresql://ubuntu:ubuntu@localhost:5432/airflowdb')

conn = engine.connect()

default_args = {
        'owner':'andrean',
        'start_date':datetime(2020,1,19),
        'email_on_failure':False,
        'email_on_retry':False,
        'retries':5,
        'retry_delay':timedelta(minutes=1),
        'depends_on_past':False
        }
dag = DAG(dag_id='coba_dag2', default_args=default_args)

query='''
SELECT DOMISILI, COUNT(DOMISILI) FROM PELANGGAN GROUP BY DOMISILI;
'''

def getquery(query):
    result = engine.execute(query)
    for row in result:
        print(row)
    result.close()

task1 = PythonOperator(
        task_id='task1',
        python_callable=getquery,
        op_kwargs={'query':query},
        dag=dag
        )
task1