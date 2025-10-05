from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random



def peace(name):
    print("Welcome" , name)


def generate_random():
    number = random.randint(1, 100)
    with open("/tmp/random.txt", "w") as f:
        f.write(str(number))
    print(f"Random number {number} saved to /tmp/random.txt")


with DAG(
    dag_id="airflow_depi",
    start_date=datetime(2025, 10, 5),
    schedule_interval=timedelta(minutes=1),
    catchup=False
) as dag:
    test_date = BashOperator(
        task_id="test1",
        bash_command="date"
    )
    test_name=PythonOperator(
        task_id="test2",
        python_callable=peace,
        op_args=["Bishoy"]      
    )
    save_random = PythonOperator(
        task_id="generate_random_number",
        python_callable=generate_random
    )

test_date>>test_name>>save_random    