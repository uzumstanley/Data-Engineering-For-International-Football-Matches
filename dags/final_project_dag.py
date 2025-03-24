from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'abdo',
    'start_date': days_ago(1),
    'catchup': False
}


with DAG(
    'spark_final_project',
    default_args=default_args,
    description='big data app DAG',
    schedule_interval=None,
) as dag:
    
    
    spark_extraction_file_path = "/home/abdo/spark_jobs/extract_job.py"
    check_data_file_path = "/home/abdo/spark_jobs/check_data_quality.py"
    create_schema_file_path = "/home/abdo/spark_jobs/transform_starschema_job.py"
    load_hdfs_file_path = "/home/abdo/spark_jobs/load_data_hdfs.py"
    
   
    extract_task = SparkSubmitOperator(
        task_id='extract_spark_data',
        application=spark_extraction_file_path,
        conn_id='spark_default',
    )
    
    
    check_data_task = SparkSubmitOperator(
        task_id='check_data_quality',
        application=check_data_file_path,
        conn_id='spark_default',
    )
    
    transorm_schema_task = SparkSubmitOperator(
        task_id='transform_star_schema_task',
        application=create_schema_file_path,
        conn_id='spark_default',
    )
    
    load_hdfs_task = SparkSubmitOperator(
        task_id='load_to_hdfs',
        application=load_hdfs_file_path ,
        conn_id='spark_default',
    )


    
    extract_task >> check_data_task >> transorm_schema_task >> load_hdfs_task
