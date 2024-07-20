from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from google_analytics_data_etl.logics import extract, transform, load


# settings message template to email for loging and monitoring error
def task_fail_email_alert(context):
    """
    Setting email content for alert notification
    :param context: dict
    :return:
    """
    subject = "[Airflow] DAG {0} - Task {1}: Failed".format(context['task_instance_key_str'].split('__')[0],
                                                            context['task_instance_key_str'].split('__')[1])
    html_content = """
    DAG: {0}<br>
    TASK: {1}<br>
    <br>
    Detail Log: <br>
    {2}
    """.format(context['task_instance_key_str'].split('__')[0],
               context['task_instance_key_str'].split('__')[1],
               context['exception'])

    send_mail = EmailOperator(
        task_id='send_email_alert',
        to="fujonfujon@gmail.com",
        subject=subject,
        html_content=html_content
    )
    return send_mail.execute(context=context)


# setting configuration dag default argumentation
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 17),
    'retries': 0,
    'timeout': 200,
    'schedule_interval': '@daily',
    'on_failure_callback': task_fail_email_alert
}


with DAG('google_analytics_data_etl', default_args=default_args, schedule_interval=None, catchup=False):

    # task extract data from file json to dict and save to xcom
    task_extraction_data = PythonOperator(
        task_id='task_extraction_data',
        python_callable=extract.extract_data,
        provide_context=True
    )

    # task transform data from json to flat table
    task_transform_to_flat = PythonOperator(
        task_id='task_transform_to_flat',
        python_callable=transform.transform_to_flat,
        provide_context=True
    )

    # task transform data from json to flat table using function group by
    task_transform_to_flat_by_operation = PythonOperator(
        task_id='task_transform_to_flat_by_operation',
        python_callable=transform.transform_to_flat_by_operation,
        provide_context=True
    )

    # task transform data dict to spreadsheet using pandas openpyxl
    task_transform_to_spreadsheet = PythonOperator(
        task_id='task_transform_to_spreadsheet',
        python_callable=transform.transform_to_spreadsheet,
        provide_context=True
    )

    # task transform data dict to spreadsheet using pandas openpyxl for group by
    task_transform_to_spreadsheet_2 = PythonOperator(
        task_id='task_transform_to_spreadsheet_2',
        python_callable=transform.transform_to_spreadsheet_2,
        provide_context=True
    )

    # task prepare query insert for input data to postgresql
    task_prepare_query_to_postgres = PythonOperator(
        task_id='task_prepare_query_to_postgres',
        python_callable=load.prepare_query_load_postgres_event,
        provide_context=True
    )

    # task insert data in postgresql using postgresql
    task_load_to_database = PostgresOperator(
        task_id='task_load_data_postgres',
        postgres_conn_id='postgres_google_analytics',
        sql="{{ ti.xcom_pull(task_ids='task_prepare_query_to_postgres', key='query_event_to_postgres') }}",
    )

    task_extraction_data >> task_transform_to_flat >> task_transform_to_spreadsheet
    task_extraction_data >> task_transform_to_flat_by_operation >> task_transform_to_spreadsheet_2 >> task_prepare_query_to_postgres >> task_load_to_database
