try:

    from datetime import timedelta
    from airflow import DAG

    from airflow.operators.python_operator import PythonOperator
    # from airflow.operators.email_operator import EmailOperator

    from datetime import datetime

    # Setting up Triggers
    from airflow.utils.trigger_rule import TriggerRule

    import uuid

    print("All Dag modules are ok ......")

except Exception as e:
    print("Error  {} ".format(e))


def first_function(**context):
    print("Hello world this works ")

def stream_data():
    import requests
    import json
    from kafka import KafkaProducer
    from kafka import KafkaConsumer
    import time
    import logging

    res=requests.get('https://randomuser.me/api/')  ##1.4/
    res=res.json()                  ### Convert to JSON format
    res=res['results'][0]
    # res=json.dumps(res, indent=3)   ### To get JSON-formatted string
    # print(res)
    res = format_data(res)
    # print(json.dumps(res, indent=3))

    # res = {
    #     'user_id': 123,
    #     'user_name': 'John Doe'
    # }

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    producer.send('users_created', json.dumps(res).encode('utf-8'))
    curr_time = time.time()

    # while True:
    #     if time.time() > curr_time + 60:  # 1 minute
    #         break
    #     try:
    #         future = producer.send('users_created', json.dumps(res).encode('utf-8'))
    #         record_metadata = future.get(timeout=10)
    #         # logging.info(
    #         #     f"Message sent successfully to topic '{record_metadata.topic}' at partition {record_metadata.partition}, offset {record_metadata.offset}")
    #         # print(type(future))
    #     except Exception as e:
    #         logging.error(f'An error occured: {e}')
    #         continue

    #     consumer = KafkaConsumer(
    #         'users_created',
    #         bootstrap_servers=['broker:29092'],
    #         auto_offset_reset='earliest',  # Start reading at the earliest message
    #         enable_auto_commit=True,
    #         group_id='my-group',
    #         value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    #     )
    #
    #     logging.info("Starting the consumer")
    #
    #     # Consume messages
    #     for message in consumer:
    #         logging.info(f"Received message: {message.value}")
    #         # Process the message (message.value contains the original data)

    return res

    # def send_message(topic, message):
    #     producer.send(topic, value=message.encode('utf-8'))
    #     producer.flush()  # Ensure all messages are sent

    # # Example usage
    # print('helllooooo')
    # send_message('my_topic', 'Hello, Kafka!')

    # # Ensure all messages are sent before exiting
    # producer.flush()

    # Close the producer
    # producer.close()
    # print(res['location']['street']['name'])
    # print(json.dumps(res, indent=3))

def format_data(res):

    data = {}   ##JSON Format
    location = res['location']
    data['emp_id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])}  {location['street']['name']}," \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


with DAG(dag_id="first_dag",
         schedule_interval= "@daily", ## timedelta(minutes=2),  ## # Schedule to run every 2 minutes ##
         default_args={
             "owner": "airflow",
             "start_date": datetime(2020, 11, 1),
             "retries": 1,
             "retry_delay": timedelta(minutes=1)
         },
         catchup=False) as dag:

    # first_function = PythonOperator(
    #     task_id="first_function",
    #     python_callable=first_function,
    # )

    stream_data = PythonOperator(
        task_id="stream_data",
        python_callable=stream_data
    )

# stream_data()