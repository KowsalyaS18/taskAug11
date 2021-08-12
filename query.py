''''THE DAG FILE'''
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from eg import *
from sum import start,end 
default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date':days_ago(5),
  'email': ['kowsalya@saturam.com'],
  'email_on_failure': True,
  'email_on_retry': True,
  'retries': 0
}
dag = DAG('query',
           default_args=default_args,
           dagrun_timeout=timedelta(minutes=5),
         # schedule_interval = '0 13 * * 1', # if you want to set up an automation schedule you can do that here
           catchup=False
)
start = PythonOperator(
     task_id='start',
     python_callable=start,
     dag=dag,
     op_kwargs={'msg': "DAG started.."}
)
alter= PythonOperator(
     task_id='alter',
     python_callable=alter,
     dag=dag,
     #op_kwargs={'msg': "hello"}
)
delete = PythonOperator(
     task_id='delete',
     python_callable=delete,
     dag=dag,
     #op_kwargs={'msg': "hello"}
)

update = PythonOperator(
     task_id='update',
     python_callable=update,
     dag=dag,
     #op_kwargs={'msg': "hello"}
)

get= PythonOperator(
     task_id='get',
     python_callable=get,
     dag=dag,
     #op_kwargs={'msg': "hello"}
)

alter_b = PythonOperator(
     task_id='alter_b',
     python_callable=alter_b,
     dag=dag,
     #op_kwargs={'msg': "hello"}
)
delete_b = PythonOperator(
     task_id='delete_b',
     python_callable=delete_b,
     dag=dag,
     #op_kwargs={'msg': "hello"}
)

update_b = PythonOperator(
     task_id='update_b',
     python_callable=update_b,
     dag=dag,
     #op_kwargs={'msg': "hello"}
)

get_b = PythonOperator(
     task_id='get_b',
     python_callable=get_b,
     dag=dag,
     #op_kwargs={'msg': "hello"}
)

end = PythonOperator(
     task_id='end',
     python_callable=end,
     dag=dag,
     op_kwargs={'msg': "DAG endeded.."}
)
start.set_downstream(alter)
start.set_downstream(alter_b)

alter.set_downstream(delete)
delete.set_downstream(update)
update.set_downstream(get)

alter_b.set_downstream(delete_b)

delete_b.set_downstream(update_b)
update_b.set_downstream(get_b)

get.set_downstream(end)
get_b.set_downstream(end)
globals()["query"] = dag


'''THE PLUGIN FILE'''
import psycopg2
import pandas as pd
from query import *
def get_connection():
    conn = psycopg2.connect(user="kowsalya",
                            password="Kowsi",
                            host="localhost",
                            port="5432",
                            database="test1")
    return conn

def alter():
    print("alter called")
    conn=get_connection()
    try:
        mycursor = conn.cursor()
    except Exception as e:
        print(e)
    try:    
        mycursor.execute(alter_table_query)
    except Exception as e:
        print(e)
    mycursor.close()
    print("executed")
def delete():
    print("delete called")
    conn=get_connection()
    try:
        mycursor = conn.cursor()
    except Exception as e:
        print(e)
    try:    
        mycursor.execute(delete_query)
    except Exception as e:
        print(e)
    mycursor.close()
    print("executed")

def update():
    print("update called")
    conn=get_connection()
    try:
        mycursor = conn.cursor()
    except Exception as e:
        print(e)
    try:    
        mycursor.execute(update_query)
    except Exception as e:
        print(e)
    mycursor.close()
    print("executed")

def get():
    df = pd.read_sql(get_query,get_connection())
    
    print (df) 




def alter_b():
    print("alter called")
    conn=get_connection()
    try:
        mycursor = conn.cursor()
    except Exception as e:
        print(e)
    try:    
        mycursor.execute(alter_table_query_b)
    except Exception as e:
        print(e)
    mycursor.close()
    print("executed")
def delete_b():
    print("delete called")
    conn=get_connection()
    try:
        mycursor = conn.cursor()
    except Exception as e:
        print(e)
    try:    
        mycursor.execute(delete_query_b)
    except Exception as e:
        print(e)
    mycursor.close()
    print("executed")

def update_b():
    print("update called")
    conn=get_connection()
    try:
        mycursor = conn.cursor()
    except Exception as e:
        print(e)
    try:    
        mycursor.execute(update_query_b)
    except Exception as e:
        print(e)
    mycursor.close()
    print("executed")

def get_b():
    df = pd.read_sql(get_query_b,get_connection())
    
    print (df) 

'''THE QUERY FILE'''
alter_table_query="ALTER TABLE usertable ADD if not exists pin INT;COMMIT;"
delete_query="DELETE FROM usertable WHERE user_id=26 ; COMMIT;"
update_query="UPDATE usertable SET company_name='kowsalya' WHERE user_id = 13;COMMIT;"
get_query="SELECT user_id,user_name,company_name FROM usertable;"

alter_table_query_b="ALTER TABLE usertable_backup ADD if not exists pin INT;COMMIT;"

delete_query_b="DELETE FROM usertable_backup WHERE user_id=26 ; COMMIT;"
update_query_b="UPDATE usertable_backup SET company_name='kowsalya' WHERE user_id = 13;COMMIT;"
get_query_b ="SELECT user_id,user_name,company_name FROM usertable_backup;"
