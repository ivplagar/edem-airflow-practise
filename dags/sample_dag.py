from airflow import DAG
from datetime import datetime
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="sample_dag",
    start_date=datetime(2022, 5, 28),
    schedule_interval=None,
    tags=["sample_dag"],
    default_args={'retries': 1},
) as dag:
    
    # Here the corresponding tasks
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)
    true_task = EmptyOperator(task_id="true_task")
    false_task = EmptyOperator(task_id="false_task")
    #print_task = BashOperator(task_id="print_task", bash_command='echo "\n\n\nHELLO DATA!!\n\n\n"',)

    @task.branch(task_id="isSaturday")
    def isSaturday():
        if datetime.today().isoweekday() == 6:
            return "true_task"
        return "false_task"

    # Here the DAG dependencies
    start >> isSaturday() >> [true_task, false_task] >> end
