import json
import numpy as np
import awkward as ak
import pika
import time
from analysis import process_file


def connecting_rabbitmq(host="rabbitmq", retries=10, delay=5):
    for attempt in range(retries):
        try:
            print(f"Connecting to RabbitMQ (attempt {attempt + 1})...")
            return pika.BlockingConnection(
                pika.ConnectionParameters(host=host, heartbeat=600, blocked_connection_timeout=300)
            )
        except pika.exceptions.AMQPConnectionError:
            print(f"RabbitMQ not ready, waiting {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to RabbitMQ after retries")


def on_job(ch, method, properties, body):

    job = json.loads(body)

    try:
        result = process_file(**job)

        masses = ak.to_numpy(result["mass"])
        weights = (
            np.ones(len(masses))
            if job["sample_name"] == "Data"
            else ak.to_numpy(result["totalWeight"])
        )

        np.savez(job["output"], masses=masses, weights=weights)

        ch.basic_publish(
            exchange="",
            routing_key="results",
            body=json.dumps({"output": job["output"]}),
        )

    except Exception as e:
        print("Error:", e)

    ch.basic_ack(delivery_tag=method.delivery_tag)


connection = connecting_rabbitmq()
channel    = connection.channel()
channel.queue_declare(queue='jobs',    durable=True)
channel.queue_declare(queue='results', durable=True)
channel.basic_qos(prefetch_count=1)  # one job at a time per worker
channel.basic_consume(queue='jobs', on_message_callback=on_job)

print("[worker] Waiting for jobs...")
channel.start_consuming()