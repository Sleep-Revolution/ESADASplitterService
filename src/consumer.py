import pika
import json
import time
import os
import src.functions.noxmultinight as SplitterService
import datetime
def process_file(message):

    file_path_zip = message['file_path_zip']
    centre = file_path_zip.split('\\')[-2]
    portal_file_path = "\\".join(file_path_zip.split('\\')[:-1])
    name = file_path_zip.split('\\')[-1].split('.')[0]
    centre_path = os.path.join(os.environ['IndividualNightWaitingRoom'],centre)

    # Check if report file exists in os.environ['IndividualNightWaitingRoom'] else make directory
    if not os.path.exists(centre_path):
        os.makedirs(centre_path)

    print("Starting night splitting process")
    Success, Message, Name = SplitterService.NoxSplitting(file_path_zip,centre_path)
    print("Ending night splitting process")
    return Success, Message, Name

def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        print(f"Received file: {message}")
        name = message["name"]
        time = datetime.datetime.now()
        message["Time"] = time.isoformat()
        ch.basic_publish(
            exchange='',
            routing_key=name,
            properties=pika.BasicProperties(
                correlation_id=properties.correlation_id
            ),
            body=json.dumps(message)
        )
        Success, Message, Name = process_file(message)
        time = datetime.datetime.now()
        message["Time"] = time.isoformat()

        ch.basic_publish(
            exchange='',
            routing_key=name,
            properties=pika.BasicProperties(
                correlation_id=properties.correlation_id
            ),
            body=json.dumps(message)
        )
        if Success:
            print("Processing completed")
        else:
            print(f"Processing failed: {Message}")

        # Acknowledge the message using the delivery tag
        
    except Exception as e:
        # Handle any exceptions that occur during processing
        print(f"Error processing message: {str(e)}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

    print("Waiting for new files. To exit, press CTRL+C")

    

def main():
    creds = pika.PlainCredentials('guest', 'guest')
    queue_name = 'file_progress_queue'
    connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_SERVER'], 5672, '/', creds, heartbeat=60*10))
    channel = connection.channel()

    channel.queue_declare(queue=queue_name)
    channel.queue_purge(queue=queue_name)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    
    print("Waiting for files. To exit, press CTRL+C")
    channel.start_consuming()
    connection.close()

if __name__ == "__main__":
    main()