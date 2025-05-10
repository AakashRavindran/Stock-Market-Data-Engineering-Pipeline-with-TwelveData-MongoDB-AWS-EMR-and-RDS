from emr_configs import EMR_cluster_info
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

job_flow_overrides = {
    "Name": "stockdata-emr-cluster",
    "ReleaseLabel": EMR_cluster_info["release_label"],
    "Applications": [{"Name": "Spark"}],
    "LogUri": EMR_cluster_info["emr_log_uri"],
    "VisibleToAllUsers": False,
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": EMR_cluster_info["instance_type"],
                "InstanceCount": 1,
            }
        ],

        "Ec2SubnetId": EMR_cluster_info["subnet_id"],
        "Ec2KeyName": EMR_cluster_info["ec2_key_name"],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,  # Setting this as false as we will terminate the cluster in another task
    },
    "JobFlowRole": EMR_cluster_info["EC2_instance_role"],
    "ServiceRole": EMR_cluster_info["EMR_service_role"],
    "BootstrapActions": [
        {
            'Name': 'Install Python Packages',
            'ScriptBootstrapAction': {
                'Path': EMR_cluster_info["bootstrap_action_s3_path"]
            }
        }
    ]

}

SPARK_STEPS = [
    {
        "Name": "MongoDataProcess",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit", "--deploy-mode", "cluster", EMR_cluster_info["spark_app_s3"]
            ],
        },
    },
]

def slack_alert_failed(context):
    ti = context.get('task_instance')
    dag_name = context.get('task_instance').dag_id
    task_name = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    dag_run = context.get('dag_run')

    mssg = f"""
        :red_circle: Pipeline Failed.        
        *Dag*:{dag_name}
        *Task*: {task_name}
        *Execution Date*: {execution_date}
        *Task Instance*: {ti}
        *Log Url*: {log_url}
        *Dag Run*: {dag_run}        
    """
    slack_notification = SlackWebhookOperator(
        task_id="slack_notification",
        slack_webhook_conn_id="slack_notifications",
        message=mssg,
        channel="#airflow_notifications"
    )
    return slack_notification.execute(context=context)


def slack_alert_succeded(context):
    ti = context.get('task_instance')
    dag_name = context.get('task_instance').dag_id
    task_name = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    dag_run = context.get('dag_run')

    mssg = f"""
            :white_tick_mark: Pipeline Completed.        
            *Dag*:{dag_name}
            *Task*: {task_name}
            *Execution Date*: {execution_date}
            *Task Instance*: {ti}
            *Log Url*: {log_url}
            *Dag Run*: {dag_run}        
        """
    slack_notification = SlackWebhookOperator(
        task_id="slack_notification",
        slack_webhook_conn_id="slack_notifications",
        message=mssg,
        channel="#airflow_notifications"
    )
    return slack_notification.execute(context=context)
