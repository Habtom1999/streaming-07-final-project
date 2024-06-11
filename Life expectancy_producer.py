"""
    This program sends a message to a queue on the RabbitMQ server.
    Messages come from a WHO Life expectancy csv file and  the messages are sent every 5 seconds.
    This program is designed to handle Life expectancy data using RabbitMQ as the message broker. 
    
    
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
QUEUE_NAME_1 = 'life_expectancy_data'  # Name of the first queue for Life expectancy with alert High Life expectancy 
QUEUE_NAME_2 = 'life_expectancy_queue1'  # Name of the second queue for high alcohol  

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

# Function to process the Life expectancy data CSV file
def process_life_expectancy_data(channel):
    dataset_path = 'Life expectancy.csv'

    with open(dataset_path, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        headers = next(csv_reader)  # Skip the header row

        for row in csv_reader:
            # Assuming the columns are in the correct order as per the headers
            country = row[0]
            year = row[1]
            life_expectancy = float(row[3])  # Change to the correct column index for life expectancy
            alcohol = float(row[4])  # Change to the correct column index for alcohol consumption

            # Check for high life expectancy 
            if life_expectancy >= 75:
                alert_message = f"ALERT: {country} in {year} has a high life expectancy of {life_expectancy}"
                send_message(channel, QUEUE_NAME_1, alert_message)

            message = f"Life Expectancy Data - Country: {country}, Year: {year}, Life Expectancy: {life_expectancy}"
            send_message(channel, QUEUE_NAME_1, message)

            # Send high alcohol consumption data to Queue 2
            if alcohol >= 10:
                alcohol_message = f"High Alcohol Consumption - Country: {country}, Year: {year}, Alcohol: {alcohol}"
                send_message(channel, QUEUE_NAME_2, alcohol_message)

            time.sleep(3)  # sleep for 3 seconds between messages

if __name__ == "__main__":
    # Ask the user if they'd like to open the RabbitMQ Admin site
    show_offer = True
    offer_rabbitmq_admin_site(show_offer)

    # Connect to RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    channel = connection.channel()

    # Declare the queues as durable
    channel.queue_declare(queue=QUEUE_NAME_1, durable=True)
    channel.queue_declare(queue=QUEUE_NAME_2, durable=True)

    process_life_expectancy_data(channel)  # Process the data once

    # Close the connection after processing
    connection.close()
