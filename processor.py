import os
import pika
import json
import time
import src.watchdog as doggo
import src.consumer as consumer
import threading


if __name__ == "__main__":
    # Create a RabbitMQ connection and channel for the consumer
    creds = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', creds))
    channel = connection.channel()
    
    # Create the FileHandler and start the Watchdog
    file_handler = doggo.FileHandler(channel)
    observer = doggo.Observer()
    observer.schedule(file_handler, path=os.environ['PortalDestination'], recursive=True)
    observer.start()
    
    # Start the RabbitMQ consumer
    consumer.main(creds)