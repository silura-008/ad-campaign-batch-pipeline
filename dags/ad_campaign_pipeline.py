from airflow.decorators import dag, task
from airflow.models import Variable

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from datetime import datetime,timedelta
import requests
import os

ENV = os.getenv("ENV", "dev")

RAW_BUCKET = f"{ENV}-ad-raw-bkt"
RAW_DB = f"{ENV}_raw_ad_db"
RAW_TABLE = "ad_table"
PROCESSED_BUCKET =  f"{ENV}-ad-processed-bkt"
PROCESSED_DB = f"{ENV}_processed_ad_db"
PROCESSED_TABLE = "iceberg_ad_table"
ATHENA_WORKGROUP = f"{ENV}-ad-analytics-wg"

da = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [Variable.get("alert_email", default_var=None)],
    'retries': 2,
    'sla': timedelta(minutes=15),
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=20),
}

@dag(dag_id="ad_campaign_pipeline",start_date=datetime(2026,1,1),schedule='@daily',catchup=False,
     dagrun_timeout=timedelta(minutes=90),max_active_runs=1,default_args=da)
def batch_etl():

# --- injest data from api and upload to s3 ----
    @task
    def ingest(ds):

        hook = S3Hook(aws_conn_id=f"aws_{ENV}")
        s3 = hook.get_client_type("s3")

        CHUNK_SIZE = 10 * 1024 * 1024  
        api_urls = {
            "facebook": f"{Variable.get('ADS_FACEBOOK_APIKEY')}&date={ds}",
            "google": f"{Variable.get('ADS_GOOGLE_APIKEY')}&date={ds}"
        }

        for name, api in api_urls.items():
            key = f"raw/ingestion_date={ds}/{name}_ads.json"
            print(f"Streaming {name}_ads.json to s3://{RAW_BUCKET}/{key}")

            upload_id = None
            parts = []
            part_number = 1

            try:
                # initiate a  multipart upload
                mpu = s3.create_multipart_upload(
                    Bucket=RAW_BUCKET,
                    Key=key,
                    ContentType="application/json"
                )
                upload_id = mpu["UploadId"]

                print(f"Started multipart upload. UploadId={upload_id}")

                with requests.get(api, stream=True, timeout=300) as r:
                    r.raise_for_status()
                    r.raw.decode_content = True

                    while True:
                        chunk = r.raw.read(CHUNK_SIZE)
                        if not chunk:
                            break

                        print(f"Uploading part {part_number}")

                        res = s3.upload_part(
                            Bucket=RAW_BUCKET,
                            Key=key,
                            UploadId=upload_id,
                            PartNumber=part_number,
                            Body=chunk
                        )

                        parts.append({
                            "PartNumber": part_number,
                            "ETag": res["ETag"]
                        })

                        part_number += 1

                # complete the multipart upload
                s3.complete_multipart_upload(
                    Bucket=RAW_BUCKET,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts}
                )

                print(f"Successfully completed upload: {key}")

            except Exception as e:
                print(f"Error during {name}_ads.json ingestion: {str(e)}")

                # clean up incomplete upload
                if upload_id:
                    try:
                        s3.abort_multipart_upload(
                            Bucket=RAW_BUCKET,
                            Key=key,
                            UploadId=upload_id
                        )
                        print(f"Aborted incomplete multipart upload for {key}")
                    except Exception as cleanup_error:
                        print(f"Cleanup failed (passed over to Lifecycle rule): {cleanup_error}")

                raise e

# --- Register partition for the data ----
    run_athena = AthenaOperator(
        task_id="register_partitions",
        query=f"""
        ALTER TABLE {RAW_DB}.{RAW_TABLE}
        ADD IF NOT EXISTS
        PARTITION (ingestion_date='{{{{ ds }}}}')
        LOCATION 's3://{RAW_BUCKET}/raw/ingestion_date={{{{ ds }}}}/';
        """,
        database= f"{RAW_DB}",
        workgroup= f"{ATHENA_WORKGROUP}",
        aws_conn_id=f"aws_{ENV}"
    )

# ---- process the data with glue ---
    run_glue = GlueJobOperator(
        task_id="run_glue_job",
        job_name=f"{ENV}-ad-glue-job",
        script_args={
            "--INGESTION_DATE": "{{ ds }}",
            "--RAW_DB":RAW_DB,
            "--RAW_TABLE": RAW_TABLE,
            "--PROCESSED_BUCKET": f"s3://{PROCESSED_BUCKET}",
            "--PROCESSED_DB": PROCESSED_DB,
            "--PROCESSED_TABLE":PROCESSED_TABLE,
        },
        aws_conn_id= f"aws_{ENV}",
        deferrable= True,
        verbose = True,
        wait_for_completion= True,
        stop_job_run_on_kill= True,
        retries = 0,
        sla= timedelta(minutes=10),
        execution_timeout=timedelta(minutes=15)

    )
    
# --- Get glue job details ----
    @task(trigger_rule= 'all_done')
    def get_job_details(**context):
        ti = context['ti']
        job_run_id = ti.xcom_pull(task_ids='run_glue_job')
        if not job_run_id:
            print("No job run ID found")
            raise ValueError(f"No job run ID found for {ENV}-ad-glue-job")
        
        hook = GlueJobHook(aws_conn_id=f"aws_{ENV}")
        glue_client = hook.get_conn()
        
        res = glue_client.get_job_run(
            JobName=f"{ENV}-ad-glue-job",
            RunId=job_run_id
        )
        
        job_run = res['JobRun']
        status = job_run.get('JobRunState', 'UNKNOWN')
        started_on = job_run.get('StartedOn')
        completed_on = job_run.get('CompletedOn')
        error_message = job_run.get('ErrorMessage', None)
        execution_time = job_run.get('ExecutionTime', 0)
        dpu_seconds = job_run.get('DPUSeconds', 0)
        max_capacity = job_run.get('MaxCapacity', job_run.get('AllocatedCapacity', 'N/A'))
        log_group = job_run.get('LogGroupName','N/A')
        attempt = job_run.get('Attempt', 'N/A')
        
        return {
            'job_name': f"{ENV}-ad-glue-job",
            'job_run_id': job_run_id,
            'status': status,
            'started_on': str(started_on) if started_on else None,
            'completed_on': str(completed_on) if completed_on else None,
            'execution_time_seconds': execution_time,
            'execution_time_minutes': f"{execution_time // 60}:{execution_time % 60:02d}",
            'dpu_seconds': dpu_seconds,
            'max_capacity': max_capacity,
            'error_message': error_message,
            'attempt': attempt,
            'log_group': log_group
        }

    ingest() >> run_athena >> run_glue >> get_job_details()

batch_etl()

