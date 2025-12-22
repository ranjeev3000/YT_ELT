from airflow import DAG
import pendulum
from datetime import timedelta, datetime
from api.video_stats import get_playlist_id, get_video_id, extract_video_data, save_to_json

from datawarehouse.dwh import staging_table, core_table

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(minutes=60),
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
    'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='A DAG to produce JSON file with raw data',
    schedule_interval='0 14 * * *',
    catchup=False,
    tags=['youtube', 'video_stats', 'ETL'],
) as dag:

    playlist_id = get_playlist_id()
    video_ids = get_video_id(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    playlist_id >> video_ids >> extracted_data >> save_to_json_task


with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='A DAG to process JSON file and insert data into both staging and core schemas',
    schedule_interval='0 15 * * *',
    catchup=False,
    tags=['youtube', 'video_stats', 'ETL'],
) as dag:

    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    # Define dependencies
    update_staging >> update_core
