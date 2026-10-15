import json
import os
import threading
import time

try:
    import pika
except ImportError:
    pika = None


def build_message(message_type, **details):
    """Create a simple JSON message for message-oriented communication."""
    message = {"message_type": message_type}
    message.update(details)
    return message


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
EXCHANGE_NAME = "hospital.events"


def send_message(host, port, message):
    """Publish a durable event; host and port remain for API compatibility."""
    if pika is None:
        return False
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, connection_attempts=1)
        )
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=message.get("message_type", "unknown").lower(),
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
        )
        connection.close()
        return True
    except (pika.exceptions.AMQPError, OSError):
        return False


def start_message_consumer(hospital_id, append_message):
    """Listen for coordinator events in a daemon thread."""
    if pika is None:
        append_message("RabbitMQ client is not installed; event consumer disabled.")
        return

    def consume():
        while True:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=RABBITMQ_HOST, connection_attempts=1)
                )
                channel = connection.channel()
                channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
                queue = channel.queue_declare(queue=f"hospital.{hospital_id}", durable=True).method.queue
                channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue, routing_key="#")

                def on_message(channel, method, properties, body):
                    append_message(f"RabbitMQ event received: {body.decode('utf-8')}")
                    channel.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_consume(queue=queue, on_message_callback=on_message)
                channel.start_consuming()
            except (pika.exceptions.AMQPError, OSError) as exc:
                append_message(f"RabbitMQ unavailable, retrying: {exc}")
                time.sleep(5)

    threading.Thread(target=consume, daemon=True).start()
