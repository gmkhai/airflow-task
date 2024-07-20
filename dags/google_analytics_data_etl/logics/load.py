def prepare_query_load_postgres_event(ti, **kwargs):
    """
    Prepare query formating for insert data to postgres database
    :param ti: Task Instance
    :param kwargs: keyword argumentation's
    :return: save to xcom object for using to PostgresOperator
    """

    # load data from x_com task_transform_to_flat_by_operation
    datas = ti.xcom_pull(key='transform_flat_group_event_name', task_ids='task_transform_to_flat_by_operation')

    # initialize list variable
    rows = []

    # mapping value data index, event_name, event_bundle_sequence_id, user_pseudo_id for insert to database
    for index, data in enumerate(datas):
        value = f"({index}, '{data['event_name']}', '{data['event_bundle_sequence_id']}', {int(data['user_pseudo_id'])})"
        rows.append(value)

    # generate string for values insert to query database
    values = ",".join(rows)
    query = f"INSERT INTO event (event_id, event_name, event_bundle_sequence_id, event_user_pseudo_count) VALUES {values}"
    ti.xcom_push(key='query_event_to_postgres', value=query)
