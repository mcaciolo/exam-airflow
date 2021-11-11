from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from train_models import train_model, train_best_model
from weather_data_etl import write_data_to_json, transform_data_into_csv

import datetime

task4_names = {
        'LinearRegression' : 'train_linear_regression',
        'DecisionTreeRegressor' : 'train_decision_tree_regressor',
        'RandomForestRegressor' : 'train_random_forest_regressor'
    }

default_args = {
    'owner': 'airflow',
    'email': ['mcaciolo@hotmail.com'],
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    dag_id='weather_dag_mcaciolo',
    description='DAG created for the airflow exam',
    tags=['datascientest', 'mcaciolo'],
    schedule_interval=datetime.timedelta(minutes=1),
    start_date=days_ago(0),
    catchup=False,
    default_args=default_args
) as weather_dag:

    task1 = PythonOperator(
        task_id='extract_weather_data',
        python_callable=write_data_to_json,
    )

    task2 = PythonOperator(
        task_id='transform_weather_data_last_20',
        python_callable=transform_data_into_csv,
        op_kwargs={'n_files':20}
    )

    task3 = PythonOperator(
        task_id='transform_weather_data_full',
        python_callable=transform_data_into_csv,
        op_kwargs={'filename':'fulldata.csv'}
    )

    task4_1, task4_2, task4_3 = [
            PythonOperator(
                task_id=task4_name,
                python_callable=train_model,
                op_kwargs={'model_name':model}
            )
            for model, task4_name in task4_names.items()
            ]

    task5 = PythonOperator(
        task_id='save_best_model',
        python_callable=train_best_model,
        op_kwargs={'task_names':task4_names}
    )

    task1 >> [task2, task3]

    [task4_1, task4_2, task4_3] << task3

    task5 << [task4_1, task4_2, task4_3]




