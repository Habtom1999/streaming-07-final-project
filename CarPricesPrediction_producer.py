"""
    This program sends a message to a queue on the RabbitMQ server.
    Messages come from a Car Prices Prediction csv file and  the messages are sent every 5 seconds.
    This program is designed to handle Car Prices data using RabbitMQ as the message broker. 
    
    
    Author: Habtom Woldu
    Date: June 07, 2024

"""

import pika
import webbrowser
import csv
import time

def offer_rabbitmq_admin_site(show_offer):
    """Offer to open the RabbitMQ Admin website"""
    if show_offer:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

# RabbitMQ server settings
HOST = 'localhost'  # Change to your RabbitMQ server address
QUEUE_NAME_1 = 'vehicle_data'  # Name of the first queue for all vehicle data 
QUEUE_NAME_2 = 'expensive_vehicles'  # Name of the second queue for expensive vehicle
QUEUE_NAME_3 = 'excellent_condition_vehicles'  # Name of the third queue for vehicles in excellent condition
QUEUE_NAME_4 = 'low_mileage_vehicles'  # Name of the forth queue for vehicles with low mileage

# Function to send a message to a RabbitMQ queue
def send_message(channel, queue_name, message):
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make the message persistent
        )
    )
    print(f"Sent to {queue_name}: {message}")

# Function to process the vehicle data CSV file
def process_Car_Prices_Prediction_data(channel):
    dataset_path = 'CarPricesPrediction.csv'

    with open(dataset_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            make = row['Make']
            model = row['Model']
            year = int(row['Year'])
            mileage = int(row['Mileage'])
            condition = row['Condition']
            price = float(row['Price'])

            # Send all vehicle data to Queue 1
            message = f"Make: {make}, Model: {model}, Year: {year}, Mileage: {mileage}, Condition: {condition}, Price: ${price}"
            send_message(channel, QUEUE_NAME_1, message)

            # Check for expensive vehicles
            if price > 25000:
                alert_message = f"ALERT: Expensive vehicle - Make: {make}, Model: {model}, Year: {year}, Price: ${price}"
                send_message(channel, QUEUE_NAME_2, alert_message)

            # Check for vehicles in excellent condition
            if condition.lower() == 'excellent':
                send_message(channel, QUEUE_NAME_3, message)

            # Check for vehicles with High mileage
            if mileage  >= 100000:
                alert_message = f"ALERT: High mileage vehicle - Make: {make}, Model: {model}, Year: {year}, Mileage: {mileage}"
                send_message(channel, QUEUE_NAME_4, alert_message)
            

            time.sleep(1)  # Sleep for 1 second

if __name__ == "__main__":
    # ask the user if they'd like to open the RabbitMQ Admin site
    show_offer = True
    offer_rabbitmq_admin_site(show_offer)

    # Connect to RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    channel = connection.channel()

    # Declare the queues as durable
    channel.queue_declare(queue=QUEUE_NAME_1, durable=True)
    channel.queue_declare(queue=QUEUE_NAME_2, durable=True)
    channel.queue_declare(queue=QUEUE_NAME_3, durable=True)
    channel.queue_declare(queue=QUEUE_NAME_4, durable=True)

    process_Car_Prices_Prediction_data(channel)  # Process the data once

    # Close the connection after processing
    connection.close()
import pika
import webbrowser
import csv
import time

def offer_rabbitmq_admin_site(show_offer):
    """Offer to open the RabbitMQ Admin website"""
    if show_offer:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

# RabbitMQ server settings
HOST = 'localhost'  # Change to your RabbitMQ server address
QUEUE_NAME_1 = 'vehicle_data'  # Name of the first queue for all vehicle data
QUEUE_NAME_2 = 'expensive_vehicles'  # Name of the second queue for expensive vehicles
QUEUE_NAME_3 = 'excellent_condition_vehicles'  # Name of the third queue for vehicles in excellent condition
QUEUE_NAME_4 = 'low_mileage_vehicles'  # Name of the forth queue for vehicles with low mileage

# Function to send a message to a RabbitMQ queue
def send_message(channel, queue_name, message):
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make the message persistent
        )
    )
    print(f"Sent to {queue_name}: {message}")

# Function to process the vehicle data CSV file
def process_Car_Prices_Prediction_data(channel):
    dataset_path = 'CarPricesPrediction.csv'

    with open(dataset_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            make = row['Make']
            model = row['Model']
            year = int(row['Year'])
            mileage = int(row['Mileage'])
            condition = row['Condition']
            price = float(row['Price'])

            # Send all vehicle data to Queue 1
            message = f"Make: {make}, Model: {model}, Year: {year}, Mileage: {mileage}, Condition: {condition}, Price: ${price}"
            send_message(channel, QUEUE_NAME_1, message)

            # Check for expensive vehicles
            if price > 25000:
                send_message(channel, QUEUE_NAME_2, message)

            # Check for vehicles in excellent condition
            if condition.lower() == 'excellent':
                send_message(channel, QUEUE_NAME_3, message)

            # Check for vehicles with low mileage
            if mileage < 20000:
                send_message(channel, QUEUE_NAME_4, message)

            time.sleep(1)  # Sleep for 1 second

if __name__ == "__main__":
    # ask the user if they'd like to open the RabbitMQ Admin site
    show_offer = True
    offer_rabbitmq_admin_site(show_offer)

    # Connect to RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    channel = connection.channel()

    # Declare the queues as durable
    channel.queue_declare(queue=QUEUE_NAME_1, durable=True)
    channel.queue_declare(queue=QUEUE_NAME_2, durable=True)
    channel.queue_declare(queue=QUEUE_NAME_3, durable=True)
    channel.queue_declare(queue=QUEUE_NAME_4, durable=True)

    process_Car_Prices_Prediction_data(channel)  # Process the data once

    # Close the connection after processing
    connection.close()
