"""
    This program listens and consumes messages from Life expectancy_producer.py
    for the Life expectancy data every 5 seconds 
    It prints the received messages along with the queue they came from.

    Author: Habtom Woldu
    Date: June 08, 2024

"""
import pika
import time

# RabbitMQ server settings
HOST = 'localhost'  # Change to your RabbitMQ server address
QUEUE_NAME_1 = 'life_expectancy_data'  # Name of the first queue for Life expectancy with alert High Life expectancy 
QUEUE_NAME_2 = 'life_expectancy_queue1'  # Name of the second queue for high alcohol  


# Function to process received messages from queue1
def process_queue1_message(ch, method, properties, body):
    print(f"Received from queue1: {body}")
    # Acknowledge the message was received and processed
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Function to process received messages from queue2
def process_queue2_message(ch, method, properties, body):
    print(f"Received from queue2: {body}")
    # Acknowledge the message was received and processed
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Function to process received messages from queue3
#def process_queue3_message(ch, method, properties, body):
    print(f"Received from queue3: {body}")
    # Acknowledge the message was received and processed
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Function to process received messages from queue4
#def process_queue4_message(ch, method, properties, body):
    print(f"Received from queue4: {body}")
    # Acknowledge the message was received and processed
    ch.basic_ack(delivery_tag=method.delivery_tag)

    time.sleep(5) ## sleep for 5 minutes

def main():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME_1, durable=True)
        channel.queue_declare(queue=QUEUE_NAME_2, durable=True)
        channel.basic_qos(prefetch_count=1)

        # Set up consumers for all four queues
        channel.basic_consume(queue=QUEUE_NAME_1, on_message_callback=process_queue1_message)
        channel.basic_consume(queue=QUEUE_NAME_2, on_message_callback=process_queue2_message)
    

        print("Consumers for all queues are waiting for messages. To exit, press CTRL+C")
        channel.start_consuming()

    except KeyboardInterrupt:
        print("\nConsumers interrupted. Goodbye.")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    main()