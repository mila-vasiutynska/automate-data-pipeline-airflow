from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    "owner": "mila",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "sparkify_data_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2019, 1, 1),
    catchup=False,  
)

# Variables
s3_bucket = Variable.get("s3_bucket")
s3_prefix = Variable.get("s3_prefix")

# Dummy start/end
start_operator = EmptyOperator(task_id="Begin_execution", dag=dag)
stop_operator = EmptyOperator(task_id="Stop_execution", dag=dag)

# Drop & create tables
drop_tables = PostgresOperator(
    task_id="Drop_tables",
    postgres_conn_id="redshift",
    sql="drop_tables.sql",
    dag=dag,
)

create_tables = PostgresOperator(
    task_id="Create_tables",
    postgres_conn_id="redshift",
    sql="create_tables.sql",
    dag=dag,
)

# Stage data
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table="staging_events",
    s3_bucket=s3_bucket,
    s3_key=f"{s3_prefix}/log-data",
    json_path=f"s3://{s3_bucket}/{s3_prefix}/log_json_path.json",
    timeformat="epochmillisecs",
    region="us-east-1",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table="staging_songs",
    s3_bucket=s3_bucket,
    s3_key=f"{s3_prefix}/song-data",
    json_path="auto",
    region="us-east-1",
)

# Load fact
load_songplays_fact_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    redshift_conn_id="redshift",
    table="songplays",
    insert_columns=[
        "songplay_id",
        "start_time",
        "userid",
        "level",
        "song_id",
        "artist_id",
        "sessionid",
        "location",
        "useragent",
    ],
    sql=SqlQueries.songplay_table_insert,
)

# Load dimensions
load_user_dim_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    truncate_before_insert=True,
)

load_song_dim_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    truncate_before_insert=True,
)

load_artist_dim_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    truncate_before_insert=True,
)

load_time_dim_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    truncate_before_insert=True,
)

# Data quality
run_data_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    tests=[
        {"check_sql": "SELECT COUNT(*) FROM songplays", "expected_result": 0, "comparison": ">"},
        {"check_sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL", "expected_result": 0},
        {"check_sql": "SELECT COUNT(*) FROM songs", "expected_result": 0, "comparison": ">"},
        {"check_sql": "SELECT COUNT(*) FROM artists", "expected_result": 0, "comparison": ">"},
    ]
)

# Dependencies
start_operator >> drop_tables >> create_tables

# Stage
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

# Dims from staging
stage_songs_to_redshift >> [load_song_dim_table, load_artist_dim_table]
stage_events_to_redshift >> load_user_dim_table

# Fact from BOTH staging tables
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_fact_table

# Time depends on songplays (per Udacity query)
load_songplays_fact_table >> load_time_dim_table

# Quality waits for everything
[
    load_songplays_fact_table,
    load_user_dim_table,
    load_song_dim_table,
    load_artist_dim_table,
    load_time_dim_table,
] >> run_data_quality_checks

run_data_quality_checks >> stop_operator
