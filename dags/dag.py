
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from utilities.utils import failure_email, success_email


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "retries": 1,
    "email_on_failure": True,
    "email_on_success": True,
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
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        try:
            # Rollback any open transaction to start fresh
            cursor.execute("ROLLBACK")

            # Execute VACUUM FULL with appropriate error handling
            cursor.execute("VACUUM FULL products")
            print(f"VACUUM FULL products completed successfully.")

            # Get table vacuum stats
            cursor.execute(
                """
                SELECT
                    schemaname,
                    relname,
                    last_vacuum,
                    last_autovacuum,
                    vacuum_count,
                    autovacuum_count
                FROM
                    pg_stat_user_tables
                WHERE
                    relname = 'products' AND schemaname = 'public'                           
            """
            )

            rows = cursor.fetchall()
            if not rows:
                print("No data found in pg_stat_user_tables for products table.")
            else:
                for row in rows:
                    print(f"Vacuum stats for {row[0]}.{row[1]}:")
                    print(f"\tLast vacuum: {row[2]}")
                    print(f"\tLast autovacuum: {row[3]}")
                    print(f"\tVacuum count: {row[4]}")
                    print(f"\tAutovacuum count: {row[5]}")

            conn.commit()
        except Exception as e:
            print(f"Error executing VACUUM FULL or fetching stats: {e}")
            raise AirflowException(
                f"Error executing VACUUM FULL or fetching stats: {e}"
            )
        finally:
            cursor.close()
            conn.close()

    def fetch_and_execute_statements():
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Execute the select query to fetch statements (replace with actual query)
            # Replace with your actual query
            cursor.execute("SELECT * FROM products")

            rows = cursor.fetchall()
            if not rows:
                print("No data found in the products table.")
            else:
                pass
                # Loop through each row and execute the statement (consider error handling)
                # for row in rows:
                #     statement = row[0]
                #     try:
                #         cursor.execute(statement)
                #         print(f"Executed statement: {statement}")
                #     except Exception as e:
                #         print(f"Error executing statement '{statement}': {e}")

            conn.commit()
        except Exception as e:
            print(f"Error fetching or executing statements: {e}")
            raise AirflowException(f"Error fetching or executing statements: {e}")
        finally:
            cursor.close()
            conn.close()

    # Define the tasks
    execute_procedure_task = PythonOperator(
        task_id="execute_stored_procedure",
        python_callable=execute_stored_procedure,
        on_success_callback=success_email,
        on_failure_callback=failure_email,
        dag=dag,
    )

    fetch_and_execute_task = PythonOperator(
        task_id="fetch_and_execute_statements",
        python_callable=fetch_and_execute_statements,
        on_success_callback=success_email,
        on_failure_callback=failure_email,
        dag=dag,
    )

    # Set task dependencies
    execute_procedure_task >> fetch_and_execute_task
