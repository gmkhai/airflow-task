import pandas as pd


def transform_to_flat(ti, **kwargs):
    """
    Transform data json from ti.xcom to flat table with matching data
    :param ti: TaskInstance
    :param kwargs: keyword arguments
    :return: xcom_push for save data in xcom object
    """

    # get data raw json from TaskInstance using key, and task_ids
    raw_data_jsons = ti.xcom_pull(key='raw_data_json', task_ids='task_extraction_data')

    # declare variable list for save mapping data to flat table
    flat_data = []

    # mapping data raw json
    for raw_data_json in raw_data_jsons:
        root_data = {
            'event_date': raw_data_json.get('event_date'),
            'event_value_in_usd': raw_data_json.get('event_value_in_usd'),
            'event_bundle_sequence_id': raw_data_json.get('event_bundle_sequence_id'),
            'event_server_timestamp_offset': raw_data_json.get('event_server_timestamp_offset'),
            'user_id': raw_data_json.get('user_id'),
            'user_pseudo_id': raw_data_json.get('user_pseudo_id'),
            'privacy_info_analytics_storage': raw_data_json.get('privacy_info',{}).get('analytics_storage'),
            'privacy_info_ads_storage': raw_data_json.get('privacy_info',{}).get('ads_storage'),
            'privacy_info_uses_transient_token': raw_data_json.get('privacy_info',{}).get('uses_transient_token'),
            'user_first_touch_timestamp': raw_data_json.get('user_first_touch_timestamp'),
            'user_ltv': raw_data_json.get('user_ltv'),
            'device_category': raw_data_json.get('device', {}).get('category'),
            'device_mobile_brand_name': raw_data_json.get('device', {}).get('mobile_brand_name'),
            'device_mobile_model_name': raw_data_json.get('device', {}).get('mobile_model_name'),
            'device_mobile_marketing_name': raw_data_json.get('device', {}).get('mobile_marketing_name'),
            'device_mobile_os_hardware_model': raw_data_json.get('device', {}).get('mobile_os_hardware_model'),
            'device_operating_system': raw_data_json.get('device', {}).get('operating_system'),
            'device_operating_system_version': raw_data_json.get('device', {}).get('operating_system_version'),
            'device_vendor_id': raw_data_json.get('device', {}).get('vendor_id'),
            'device_advertising_id': raw_data_json.get('device', {}).get('advertising_id'),
            'device_language': raw_data_json.get('device', {}).get('language'),
            'device_is_limited_ad_tracking': raw_data_json.get('device', {}).get('is_limited_ad_tracking'),
            'device_time_zone_offset_seconds': raw_data_json.get('device', {}).get('time_zone_offset_seconds'),
            'device_browser': raw_data_json.get('device', {}).get('browser'),
            'device_browser_version': raw_data_json.get('device', {}).get('browser_version'),
            'device_web_info': raw_data_json.get('device', {}).get('web_info'),
            'geo_continent': raw_data_json.get('geo', {}).get('continent'),
            'geo_country': raw_data_json.get('geo', {}).get('country'),
            'geo_region': raw_data_json.get('geo', {}).get('region'),
            'geo_city': raw_data_json.get('geo', {}).get('city'),
            'geo_sub_continent': raw_data_json.get('geo', {}).get('sub_continent'),
            'geo_metro': raw_data_json.get('geo', {}).get('metro'),
            'stream_id':  raw_data_json.get('stream_id'),
            'platform':  raw_data_json.get('platform'),
            'event_dimensions':  raw_data_json.get('event_dimensions'),
            'ecommerce':  raw_data_json.get('ecommerce'),
        }

        # save data in variable list
        flat_data.append(root_data)

    # list data generate to datafraame
    data_frame = pd.DataFrame.from_records(flat_data)

    # preprocessing data delete columns if the columns have null value
    data_frame_not_null = data_frame.dropna(axis=1, how='all')

    # group by data by 'event_bundle_sequence_id', 'geo_city' and count 'user_pseudo_id' for get information many user pseodo for each geo city and event_bundle_sequence_id
    data = data_frame_not_null.groupby(['event_bundle_sequence_id', 'geo_city']).aggregate({'user_pseudo_id':'count'})

    # change dataframe to dictionary because xcom can't save data object or tuple data
    data_to_dict = data.reset_index().to_dict('records')
    ti.xcom_push(key='transform_flat_group_geo_city', value=data_to_dict)
    

def transform_to_flat_by_operation(ti, **kwargs):
    """
    Transform data to flat using operation with pure data raw
    :param ti: TaskInstance
    :param kwargs: keyword argumentation
    :return: x_com_push
    """
    # get data raw json from TaskInstance using key, and task_ids
    raw_data_jsons = ti.xcom_pull(key='raw_data_json', task_ids='task_extraction_data')

    # raw data generate to dataframe
    data_frame = pd.DataFrame.from_records(raw_data_jsons)

    # raw dataframe group by 'event_name', 'event_bundle_sequence_id' for get information count user_pseudo
    data = data_frame.groupby(['event_name', 'event_bundle_sequence_id']).aggregate({'user_pseudo_id': 'count'})

    # change dataframe to dict for save to scom_push function
    data_to_dict = data.reset_index().to_dict('records')
    ti.xcom_push(key='transform_flat_group_event_name', value=data_to_dict)


def transform_to_spreadsheet(ti, **kwargs):
    """
    Function to transform raw data mapped to excel document
    :param ti: Task Instance
    :param kwargs: keyword argumentation's
    :return: save transformed data using group by to local directory for transform raw data with mapping
    """

    # get data transformed raw data with type dct  from x_com
    data_flat = ti.xcom_pull(key='transform_flat_group_geo_city', task_ids='task_transform_to_flat')

    # transform to dataframe
    data_frame = pd.DataFrame(data_flat)

    # export data to excel document
    data_frame.to_excel('dags/google_analytics_data_etl/datas/data_flat_group_by_geo_city.xlsx')


def transform_to_spreadsheet_2(ti, **kwargs):
    """
    Function to transform raw data (not mapped) to excel document
    :param ti: Task Instance
    :param kwargs: keyword argumentation's
    :return: save transformed data using group by to local directory for transform data
    """
    # get data transformed raw data with type dct  from x_com
    data_flat = ti.xcom_pull(key='transform_flat_group_event_name', task_ids='task_transform_to_flat_by_operation')

    # transform to dataframe
    data_frame = pd.DataFrame(data_flat)

    # export data to excel document
    data_frame.to_excel('dags/google_analytics_data_etl/datas/data_flat_group_by_event_name.xlsx')


