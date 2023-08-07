from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import datetime

dag = DAG(
    dag_id="etl_dag",
    schedule_interval="@daily",
    start_date=datetime.datetime(2023, 8, 8),
)

def transform_data():
    """Transforms the data in the input directory and saves it to the output directory."""

    df = pd.read_excel(r"C:\Users\User\Documents\Data Engineer Test - Raizen\solution\vendas-combustiveis-m3.xlsx")

    df["year_month"] = pd.to_datetime(df["year_month"])

    df = df.set_index("year_month")

    df.to_parquet(r"C:\Users\User\Documents\Data Engineer Test - Raizen\solution\data.parquet")

transform_data_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

extract_data_task = BashOperator(
    task_id="extract_data",
    bash_command="python etl.py",
    dag=dag,
)

load_data_task = BashOperator(
    task_id="load_data",
    bash_command="python functions/load_data.py",
    dag=dag,
)

extract_data_task >> transform_data_task >> load_data_task
