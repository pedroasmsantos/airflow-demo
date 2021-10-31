from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
from pandas import json_normalize

import json

default_args = {
	'start_date': datetime(2021, 9, 13)
}

def processing_user(task_instance):
    users = task_instance.xcom_pull(task_ids=['extracting_user'])
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    
    user = users[0]['results'][0]
    user_email = user['email']
    user_first_name = user['name']['first']
    user_last_name = user['name']['last']
    user_country = user['location']['country']

    processed_user = json_normalize({
        'firstname' : user_first_name,
        'lastname' : user_last_name,
        'country' : user_country,
        'username' : user['login']['username'],
        'password' : user['login']['password'],
        'email' : user_email
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=True)

    task_instance.xcom_push(key='user_email', value=user_email)
    task_instance.xcom_push(key='user_first_name', value=user_first_name)
    task_instance.xcom_push(key='user_last_name', value=user_last_name)
    task_instance.xcom_push(key='user_country', value=user_country)

def produce_message_to_kafka(user_email, user_first_name, user_last_name, user_country):
    from kafka import KafkaProducer
    import json
    
    context = {
		'bootstrap': ['kafka:9092'],
        'topic': 'dev.users_updated',
        'key': user_email,
        'value':[ { "email": user_email, "firstname": user_first_name, "lastname": user_last_name, "country": user_country} ]
    }

    topic_name = context['topic']
    key = context['key']
    value = context['value']

    producer_instance = KafkaProducer(bootstrap_servers=context['bootstrap'], api_version=(0, 10))
    
    try:
        producer_instance.send(topic_name, bytes(json.dumps(context['value']), 'utf-8'), bytes(context['key'], 'utf-8'))
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception thrown while publishing message')
        print(str(ex))
        raise Exception(str(ex))

with DAG('user_processing_example', schedule_interval='@daily', 
	default_args = default_args, catchup=False) as dag:

	creating_table = PostgresOperator(
		task_id = 'creating_table',
		postgres_conn_id = 'postgres',
		sql='''
			CREATE TABLE IF NOT EXISTS users (
				email TEXT NOT NULL PRIMARY KEY,
				firstname TEXT NOT NULL,
				lastname TEXT NOT NULL,
				country TEXT NOT NULL,
				username TEXT NOT NULL,
				password TEXT NOT NULL
			);
			'''
	)

	is_api_available = HttpSensor(
		task_id='is_api_available',
		http_conn_id='user_api',
		endpoint='api/'
	)

	extracting_user = SimpleHttpOperator(
		task_id='extracting_user',
		http_conn_id='user_api',
		endpoint='api/',
		method='GET',
		response_filter=lambda response: json.loads(response.text),
		log_response=True
	)

	processing_user = PythonOperator(
		task_id='processing_user',
		python_callable=processing_user
	)
	
	storing_user = BashOperator(
		task_id='storing_user',
		bash_command='cat /tmp/processed_user.csv | tr -s "," | psql postgresql://airflow:airflow@postgres:5432/airflow -c "COPY users FROM STDIN CSV HEADER"'
	)

	notify_user_changes = PythonVirtualenvOperator(
        task_id='notify_user_changes',
        python_callable=produce_message_to_kafka,
        requirements=["kafka-python", "confluent-kafka"],
        op_kwargs={'user_email': '{{ ti.xcom_pull(key="user_email") }}', 'user_first_name': '{{ ti.xcom_pull(key="user_first_name") }}', 
                   'user_last_name': '{{ ti.xcom_pull(key="user_last_name") }}', 'user_country': '{{ ti.xcom_pull(key="user_country") }}'}
    )

	creating_table >> is_api_available >> extracting_user >> processing_user >> storing_user >> notify_user_changes
