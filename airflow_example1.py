import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

#if you use depends_on_past=True, individual task instances will depend on the success of their previous task instance (that is, previous according to execution_date)
default_args = {
    'owner':'andrean',
    'start_date':datetime(2020,1,10),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past':False 
}
dag = DAG(dag_id='coba_dag', default_args=default_args)

a=1
b=2
def sumnumber(x,y):
  return x+y

task1 = BashOperator(
    task_id='task1',
    bash_command='echo task1 run success',
    dag=dag
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=sumnumber,
    provide_context=True,
    op_kwargs={'x':a,'y':b}
)

task1 >> task2