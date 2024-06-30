import os
from airflow.utils.email import send_email

def get_log_content(task_instance, try_number):
    log_base_path = 'logs' 
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    run_id = task_instance.run_id
    
    log_file_path = os.path.join(
        log_base_path,
        f"dag_id={dag_id}",
        f"run_id={run_id}",
        f"task_id={task_id}",
        f"attempt={try_number}.log"
    )
    
    try:
        with open(log_file_path, 'r') as log_file:
            return log_file.read()
    except Exception as e:
        return f'Failed to read log file: {e}'
    
    
    
def send_task_status_email(context, task_status):
    task_instance = context['task_instance']
    try_number = task_instance.prev_attempted_tries
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    log_content = get_log_content(task_instance, try_number)
    body = (f'The task {task_instance.task_id} completed with status: {task_status}.\n\n'
                f'The task execution date is: {context["execution_date"]}\n\n'
                f'Log url: {task_instance.log_url}\n\n'
                f'Log content:\n\n{log_content}\n\n')
    
    to_email = 'xxxxxx@gmail.com' #recipient mail
    send_email(to=to_email, subject=subject, html_content=body)

def success_email(context):
    send_task_status_email(context, 'Success')

def failure_email(context):
    send_task_status_email(context, 'Failed')