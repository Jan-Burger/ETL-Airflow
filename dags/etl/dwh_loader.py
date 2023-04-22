from airflow.decorators import task

@task()
def extract_data_from_staging_area():
    print("Staging ETL process DONE!!! ------- Following DAG succesfully triggerd")
