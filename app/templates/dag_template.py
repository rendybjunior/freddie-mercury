from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_args = {
    'owner': '{{ owner }}',
    'depends_on_past': False,
    'start_date': datetime.strptime('{{ start_date }}','%Y-%m-%d'),
    'email': '{{ email }}',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': {{ retries }},
    'retry_delay': timedelta(minutes={{ retry_delay_minutes }}),
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
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    '{{ dag_name }}',
    default_args=default_args,
    description='{{ description }}',
    schedule_interval='{{ schedule_interval }}',
)

{% for task in tasks %}
{% if task['type'] == 'BASH' %}
{{ task['task_id'] }} = BashOperator(
    task_id='{{ task["task_id"] }}',
    bash_command='{{ task["bash_command"] }}',
    dag=dag,
)
{% endif %}
{% if task['type'] == 'BQ_ETL' %}
{{ task['task_id'] }} = BigQueryOperator(
    sql='{{ task["sql"] }}',
    destination_dataset_table='{}'.format('{{ task["destination_table"]}}' + {% raw %}'${{ tomorrow_ds_nodash }}'{% endraw %}),
    task_id='{{ task["task_id"] }}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    dag=dag,
    use_legacy_sql=False,
    time_partitioning={'type': 'DAY'},
    labels={
        'owner': '{{ owner }}'.lower(),
        'task_id': '{{ task["task_id"] }}'.lower(),
        'execution_date': {% raw %}'{{ tomorrow_ds_nodash }}'{% endraw %}
    })
{% endif %}
{% endfor %}

{% for dependency in dependencies %}
{{ dependency['preceding_task_id'] }} >> {{ dependency['task_id'] }}
{% endfor %}