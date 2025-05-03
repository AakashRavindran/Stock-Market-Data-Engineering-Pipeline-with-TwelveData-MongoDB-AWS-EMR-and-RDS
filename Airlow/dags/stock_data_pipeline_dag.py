from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
import boto3
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.sensors.rds import RdsDbSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

import sys
import os
sys.path.append('/home/ubuntu/airflow_python_scripts/DAG_Full_Code')
# Below  python file has the required configs for the Operators
import dag_info


@dag(
    dag_id="GUVI_stock_data_pipeline",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    on_failure_callback=dag_info.slack_alert_failed,
    on_success_callback=dag_info.slack_alert_succeded
)
def stock_data_dag():
    # Task 1: Load stock data to MongoDB via SSH
    load_stockdata_to_mongo = SSHOperator(
        task_id="load_stockdata_to_mongo",
        ssh_conn_id='mongoload_Ec2_connection',
        command='<run bash command to start the Python Script to load to MongoDB>'
    )
    # Task 2: Create an EMR Cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=dag_info.job_flow_overrides,
        region_name="ap-south-1",
        aws_conn_id="aws_project_connection"
    )

    # Task 3 : Validate EMR Cluster has been created
    is_emr_cluster_created = EmrJobFlowSensor(
        task_id="is_emr_cluster_created",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        target_states={"WAITING"},  # This sensor waits till the EMR job flow reaches this state mentioned here
        timeout=3600,
        poke_interval=5,
        aws_conn_id="aws_project_connection",
        mode='poke',
    )

    # Task 4 : Add step to EMR Cluster
    add_stock_data_process = EmrAddStepsOperator(
        task_id="add_stock_data_process_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_project_connection",
        steps=dag_info.SPARK_STEPS
        # do_xcom_push=True, # If required Enable XCom push to monitor step status
    )
    # Task 5 : EMR Sensor to check if step completed
    is_transformation_completed = EmrStepSensor(
        task_id="is_EMR_step_completed",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_stock_data_process_step')[0] }}",
        target_states={"COMPLETED"},  # ensure that this state is in UPPER case
        timeout=3600,
        poke_interval=10,
        aws_conn_id="aws_project_connection"
    )

    # Task 6 : Terminate EMR Cluster
    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_EMR_cluster",
        aws_conn_id="aws_project_connection",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    )

    # Task 7 : EMR Sensor to check cluster has been terminated
    is_emr_cluster_terminated = EmrJobFlowSensor(
        task_id="is_emr_cluster_terminated",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        target_states={"TERMINATED"},  # ensure that this state is in UPPER case
        timeout=3600,
        poke_interval=5,
        mode='poke',
        aws_conn_id="aws_project_connection"
    )

    # Task 8 : RDS Sensor to check RDS Status
    await_db_instance = RdsDbSensor(
        task_id="await_db_instance",
        db_identifier="<RDS Database Identifier>",
        aws_conn_id="aws_project_connection",
        poke_interval=30,
        timeout=3600,
        mode='poke',
    )

    # Task 9 : Start pre created Glue Job
    start_glue_job = GlueJobOperator(
        task_id="start_glue_job",
        job_name="<glue_job>",
        aws_conn_id="aws_project_connection"
    )

    # Task 10 : Glue Job Sensor checks if glue job completed
    check_glue_job_completion = GlueJobSensor(
        task_id="check_glue_job_completion",
        job_name="<glue_job>",
        run_id="{{ task_instance.xcom_pull(task_ids='start_glue_job') }}",
        aws_conn_id="aws_project_connection",
        poke_interval=30,
        timeout=3600,
    )

    load_stockdata_to_mongo >> create_emr_cluster >> is_emr_cluster_created >> add_stock_data_process >> is_transformation_completed >> remove_cluster >> is_emr_cluster_terminated >> await_db_instance >> start_ >


# Instantiate the DAG
my_stock_data_dag = stock_data_dag()