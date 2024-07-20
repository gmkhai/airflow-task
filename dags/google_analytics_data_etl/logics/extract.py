import json


def extract_data(ti, **kwargs):
    """
    Extract data from json file to object x_com for airflow
    :param ti: Task Instance
    :param kwargs: keyword argumentation's
    :return: xcom_push for save data in xcom object
    """

    # using function open for access raw data json
    with open('dags/google_analytics_data_etl/datas/Raw_Data.json', 'r') as file:
        data = json.load(file)

    # save to scom airflow object
    ti.xcom_push(key='raw_data_json', value=data)

