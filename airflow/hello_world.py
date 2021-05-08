# coding:utf-8

"""
### Hello World
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""

# [START tutorial]
# [START import_module]
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,               # 是否依赖上一个自己的执行状态，这个地方不推荐设置成True，否则手动执行会一直卡住，应为上次执行未成功。自动执行没有问题。
    'email': ['airflow@example.com'],       # 接收通知的email列表
    'email_on_failure': False,              # 是否在任务执行失败时接收邮件
    'email_on_retry': False,                # 是否在任务重试时接收邮件
    'retries': 3,                           # 失败重试次数
    'retry_delay': timedelta(minutes=1),    # 失败重试间隔
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2030, 12, 31),  # 调度结束时间，只有该时间段才会执行
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'  # 该参数很关键
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    dag_id='hello_world',
    default_args=default_args,
    description='A simple tutorial DAG, Hello World.',
    start_date=days_ago(1),              # 必须，否则报错airflow.exceptions.AirflowException: Task is missing the start_date parameter
    # schedule_interval="00, *, *, *, *"  # 执行周期，依次是分，时，天，月，年，此处表示每个整点执行
    schedule_interval=timedelta(days=1),  # 执行周期，表示每小时执行一次，也可以使用crontab格式设置
    tags=['hello_world'],
)
# [END instantiate_dag]

# t1, t2 and t3 are examples of tasks created by instantiating operators
# [START basic_task]
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag,
)
# [END basic_task]

# [START documentation]
dag.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

I'm testing 1...
"""
# [END documentation]

t3 = BashOperator(
    task_id='echo',
    bash_command='echo "Hello World, `whoami`."; pwd;',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

t4 = BashOperator(
    task_id='execute_script',
    bash_command="bash /home/work/warehouse/dws/dim_holidays.sh -p funan -i 1",
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)


t5 = BashOperator(
    task_id='all_done',
    bash_command="echo all done.",
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

t1 >> [t2, t3] >> t4 >> t5
# [END tutorial]
