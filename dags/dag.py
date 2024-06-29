from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="airflow_for_etl",
    schedule_interval="@weekly",
    start_date=datetime.now(),
    max_active_runs=1,
    default_args=default_args,
    template_searchpath="plugins/scripts/sql",
    catchup=True,
) as dag:
    """
    Using mock local database while developing
    """
    
    def execute_stored_procedure():
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Rollback any open transaction to start fresh
        cursor.execute("ROLLBACK")
        cursor = conn.cursor()
        
        """
        Since VACUUM cannot be invoked by a function 
        Changing the function into a procedure does not solve the problem
        Link docs: https://fluca1978.github.io/2020/02/06/VacuumByNotOwners.html
        """
        cursor.execute("VACUUM FULL products")
        conn.commit()
        
        cursor.close()
        conn.close()

    def fetch_and_execute_statements():
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Execute the select query to fetch the statements from the temp table
        cursor.execute("SELECT * FROM products")
        
        # Row 58 to 68 cannot be executed because I'm using mock database for now

        # Fetch all rows from the result set
        # rows = cursor.fetchall()

        # Loop through each row and execute the statement
        # for row in rows:
        #     # Extract the statement from the row
        #     statement = row[0]
            
        #     # Execute the statement
        #     cursor.execute(statement)
        #     print("Executed statement:", statement)
            
        # Commit the transaction
        conn.commit()

        cursor.close()
        conn.close()

    # Define the tasks
    execute_procedure_task = PythonOperator(
        task_id='execute_stored_procedure',
        python_callable=execute_stored_procedure,
        dag=dag,
    )

    fetch_and_execute_task = PythonOperator(
        task_id='fetch_and_execute_statements',
        python_callable=fetch_and_execute_statements,
        dag=dag,
    )

    # Set task dependencies
    execute_procedure_task >> fetch_and_execute_task
        

