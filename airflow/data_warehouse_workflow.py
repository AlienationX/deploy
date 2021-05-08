# coding:utf-8

"""
### Example DAG demonstrating the usage of the TaskGroup. 
eg:

    Data Warehouse
    Data Sync
    ... ...
"""

olap_md="""\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

I'm testing 1...
这个如何查看呢？
"""

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

import pendulum
import sys
from datetime import datetime, timedelta

# [START global variable]
# DEFAULT_START_DATE = pendulum.yesterday()
DEFAULT_START_DATE = pendulum.local(2010, 1, 1).to_date_string()
DEFAULT_END_DATE = pendulum.yesterday().to_date_string()

PROJECT_ROOT = "/home/work"
PROJECT_WAREHOUSE = f"{PROJECT_ROOT}/azkaban_warehouse"
PROJECT_ODS = f"{PROJECT_WAREHOUSE}/ods"
PROJECT_DWB = f"{PROJECT_WAREHOUSE}/dwb"
PROJECT_DWS = f"{PROJECT_WAREHOUSE}/dws"
PROJECT_SHARE = f"{PROJECT_WAREHOUSE}/share"

COMMAND_PYTHON3 = "/home/work/app/python3/bin/python3"
COMMAND_DATAX = "python /home/work/app/datax/bin/datax.py"

RESULT_STATE = "success"

params = {
    "project": "",
    "day": "day",
    "week": "week",
    "month": "month",
    "quarter": "quarter",
    "year": "year",
    "start_date": "",
    "end_date": "",
    "init": "0",
    "write_log": "1",
    "send": "1",
}
# [END global variable]


default_args = {
    "owner": "airflow",
    "depends_on_past": False,               # 是否依赖上一次dag的执行状态，这个地方不推荐设置成True，否则手动执行会一直卡住，应为上次执行未成功。自动执行没有问题。
    "email": ["airflow@example.com"],       # 接收通知的email列表
    "email_on_failure": False,              # 是否在任务执行失败时接收邮件
    "email_on_retry": False,                # 是否在任务重试时接收邮件
    "retries": 3,                           # 失败重试次数
    "retry_delay": timedelta(minutes=1),    # 失败重试间隔
    "catchup": False,                       # 不回补历史
    # "dag_concurrency": 1,                   # 调度器允许并发运行的任务实例的数量？ 并行数限制和parallelism的区别？未测试
    # "max_active_runs_per_dag": 1            # 每个DAG的最大活动DAG运行次数
    # "trigger_rule": "all_success"           # 该参数很关键，默认all_success
    "params": params,                       # web页面会自动加载这些参数，可以执行并修改即可。命令行执行的直接传入下面的参数加修改的即可。
}


def failure_callback():
    # 未成功，还需测试！！！
    # TODO
    global RESULT_STATE
    RESULT_STATE = "failure"
    print("todo something")


# [START howto_task_group]
with DAG (
    dag_id="data_warehouse_workflow",
    description="A full data warehouse work flow example.",
    default_args=default_args,
    start_date=pendulum.now().set(hour=0, minute=5, second=0),  # 每天的00:05:00执行，执行时间为：start_date + schedule_interval
    # start_date=pendulum.yesterday(),           # 使用pendulum可以解决时区问题，强烈推荐
    # start_date=days_ago(1),                    # 必须，否则报错airflow.exceptions.AirflowException: Task is missing the start_date parameter
    # schedule_interval="05 00 * * *",           # 执行周期，依次是分，时，天，月，年，此处表示每天00:05执行
    # schedule_interval=timedelta(hours=1),      # 切记执行时间是：start_date + schedule_interval
    schedule_interval="@daily",
    on_failure_callback=failure_callback,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    tags=["datawarehouse"],
) as dag:
    # 设置文档
    dag.doc_md = __doc__

    # ------------------------------------------------ 参数处理
    # params_dict = default_args.get("params") # 只能获取上面写死的固定值，running状态的值无法获取，不推荐

    # Jinja模板语言，只有运行时才会赋值，且暂时不知道如何修改
    run_project = "{{ dag_run.conf['project'] }}"
    run_start_date = "{{ dag_run.conf['start_date'] }}"
    run_end_date = "{{ dag_run.conf['end_date'] }}"
    run_init = "{{ dag_run.conf['init'] }}"
    
    start_date = ""

    # 默认值推荐使用单独脚本设置并传递，这里传递单独执行任务脚本时会比较麻烦，需要额外配置参数的默认值
    # 并且该方式无法赋值，因为Jinja只有在下面执行的时候才会渲染并赋值......暂时使用全局变量params来实现
    # if start_date=="" or start_date is None:
    #     start_date = "2000-01-01"

    # end_date = "9999-12-31"
    
    print(run_project, run_start_date, run_end_date, run_init)

    # ------------------------------------------------
    def check_params(*args, **kwargs):
        print("-" * 20 , "START", "-" * 20)
        for k, v in kwargs.items():
            print(str(k) + " = " + str(v))
        print("-" * 20 , "END", "-" * 20)

        from pprint import pprint
        pprint(args)
        pprint(kwargs)

        # 获取默认传入的dag_run字典参数
        print("project =", kwargs.get("dag_run").conf.get("project"))
        print("start_date =", kwargs.get("dag_run").conf.get("start_date"))
        print("end_date =", kwargs.get("dag_run").conf.get("end_date"))
        print("PROJECT_WAREHOUSE =", PROJECT_WAREHOUSE)

        # 获取自定义函数的变量
        print("my param:", kwargs.get("my_param"))
        print("my param convert_project:", kwargs.get("convert_project"))

        # 参数校验，未通过则抛出异常
        if not kwargs.get("dag_run").conf.get("project"):
            raise Exception("Must be input project param !")

            # # 都会触发重试次数，推荐使用raise
            # print("Must be input project param !")
            # sys.exit(1)
        
        # 各别参数为空，赋予默认值。无法赋值到全局变量，未解决。推荐默认值在脚本里单独处理！！！
        global params
        params = kwargs.get("dag_run").conf
        # params = kwargs["params"]

        if not params["start_date"]:
            params["start_date"] = DEFAULT_START_DATE

        if not params["end_date"]:
            params["end_date"] = DEFAULT_END_DATE

        pprint(params)
        # return params
    # ------------------------------------------------ 参数处理

    # START
    start = DummyOperator(task_id="start")

    prepare_stage = PythonOperator(
        task_id="prepare_stage", 
        python_callable=check_params,
        op_args=[1, "hello world"],  # 给函数传入列表参数
        op_kwargs={
            "my_param": "Parameter I passed in",
            "convert_project": run_project,
        },  # 给函数传入字典参数
        # provide_context=True,  # provide_context is deprecated as of 2.0 and is no longer required
    )

    # import json
    # import ast
    # print(prepare_stage.output)
    # params = json.loads(str(prepare_stage.output))
    # params = json.loads("{{ task_instance.xcom_pull(task_ids='prepare_stage', key='return_value') }}")
    # params = eval("{{ task_instance.xcom_pull(task_ids='prepare_stage', key='return_value') }}")
    # params = ast.literal_eval("{{ task_instance.xcom_pull(task_ids='prepare_stage') }}")

    with TaskGroup("import_std", tooltip="Tasks for import_std") as import_std:
        task_1 = BashOperator(task_id="task_1", bash_command=f'echo "bash {PROJECT_SHARE}/std_organization.sh"')
        task_2 = BashOperator(task_id="task_2", bash_command="echo 2")
        task_3 = BashOperator(task_id="task_3", bash_command="echo 3")

    with TaskGroup("data_sync", tooltip="Tasks for data_sync") as data_sync:
        task_1 = BashOperator(task_id="task_1", bash_command="echo 1")
        task_2 = BashOperator(task_id="task_2", bash_command="echo 2")
        task_3 = BashOperator(task_id="task_3", bash_command="echo 3")

        task_1 >> [task_2, task_3]

    with TaskGroup("http_sync", tooltip="Tasks for http_sync") as http_sync:
        task_1 = BashOperator(task_id="task_1", bash_command="echo 1")
        task_2 = BashOperator(task_id="task_2", bash_command="echo 2")
        task_3 = BashOperator(task_id="task_3", bash_command="echo 3")
        task_4 = BashOperator(task_id="task_4", bash_command="echo 4")

        [task_1, task_2] >> task_3 >> task_4

    # [START howto_task_group_ods]
    with TaskGroup("ods", tooltip="Tasks for ods") as ods:
        task_1 = DummyOperator(task_id="task_1")

        # [START howto_task_group_inner_ods]
        with TaskGroup("inner_ods", tooltip="Tasks for inner_section2") as inner_ods:
            dcg_yb_master_info = BashOperator(task_id="dcg_yb_master_info", bash_command=f""" echo "bash {PROJECT_ODS}/dcg_yb_master_info.sh -p {params["project"]} -s{params["start_date"]} -e{params["end_date"]} -d {params["month"]}" """)
            ods_yb_master_info = BashOperator(task_id="ods_yb_master_info", bash_command=f'echo "bash {PROJECT_ODS}/ods_yb_master_info.sh -p {run_project} -s{run_start_date} -e{run_end_date}"')
            ods_yb_charge_detail = BashOperator(task_id="ods_yb_charge_detail", bash_command=f'echo "bash {PROJECT_ODS}/ods_yb_charge_detail.sh -p {run_project} -s{run_start_date} -e{run_end_date}"')
            ods_yb_settlement = BashOperator(task_id="ods_yb_settlement", bash_command=f'echo "bash {PROJECT_ODS}/ods_yb_settlement.sh -p {run_project} -s{run_start_date} -e{run_end_date}"')

            dcg_yb_master_info >> [ods_yb_master_info, ods_yb_charge_detail, ods_yb_settlement]
        # [END howto_task_group_inner_ods]

        # 不写依赖就是并行执行
        # task_1 >> inner_ods

    # [END howto_task_group_ods]

    with TaskGroup("dw", tooltip="Tasks for dw") as dw:
        task_1 = BashOperator(task_id="task_1", bash_command="echo 1")
        task_2 = BashOperator(task_id="task_2", bash_command="sleep 5")
        task_3 = BashOperator(task_id="task_3", bash_command="echo 3")

        task_1 >> [task_2, task_3]

    with TaskGroup("dm", tooltip="Tasks for dm") as dm:
        task_1 = BashOperator(task_id="task_1", bash_command="echo 1")
        task_2 = BashOperator(task_id="task_2", bash_command="echo 2")
        task_3 = BashOperator(task_id="task_3", bash_command="echo 3")

        task_1 >> [task_2, task_3]

    with TaskGroup("olap", tooltip="Tasks for olap") as olap:
        task_1 = BashOperator(task_id="task_1", bash_command="echo 1")
        task_2 = BashOperator(task_id="task_2", bash_command="exit 2")  # 抛出异常，为了触发on_failure_callback进行测试
        task_3 = BashOperator(task_id="task_3", bash_command="exit 3")
        task_4 = BashOperator(task_id="task_4", bash_command="echo 4")

        task_1 >> [task_2, task_3] >> task_4
        olap.doc_md = olap_md

    with TaskGroup("data_export", tooltip="Tasks for data_export") as data_export:
        task_1 = BashOperator(task_id="task_1", bash_command="sleep 3")
        task_2 = BashOperator(task_id="task_2", bash_command="sleep 3")
        task_3 = DummyOperator(task_id="task_3")

        task_1 >> [task_2, task_3]

    # all_done，无论成功失败都执行
    send_email = BashOperator(task_id="send_email", bash_command=f'echo "send email {RESULT_STATE}"', trigger_rule="all_done")

    end = DummyOperator(task_id="end")

    start >> prepare_stage
    prepare_stage >> import_std >> dw
    prepare_stage >> [data_sync, http_sync] >> ods >> dw
    dw >> data_export >> send_email
    dw >> dm >> olap >> send_email
    send_email >> end
# [END howto_task_group]
