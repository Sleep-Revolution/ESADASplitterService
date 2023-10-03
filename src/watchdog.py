import os
import pika
import json
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

def send_file_to_queue():
    def process_file(file_path):
        name = file_path.split('\\')[-1].split('.')[0]
        message = {
            "file_path_zip": file_path,
            "name": name
        }

        # Ensure that the message is published before returning
        try:
            # Ensure that the message is published before returning
            channel.basic_publish(exchange='', routing_key="file_progress_queue", body=json.dumps(message))
            print(f"Sent '{message}' for 3 Nights Splitting process")
        except pika.exceptions.StreamLostError as e:
            # Handle stream lost error gracefully
            print(f"Stream connection lost: {e}")
            reconnect()
            channel.basic_publish(exchange='', routing_key="file_progress_queue", body=json.dumps(message))

    def reconnect():
        while True:
            try:
                # Attempt to establish a new connection
                print("Reconnecting to RabbitMQ...")
                connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_SERVER']))
                channel = connection.channel()
                channel.queue_declare(queue=queue_name)
                channel.queue_purge(queue=queue_name)

                channel.basic_qos(prefetch_count=1)

                # Re-register the observer if reconnection is successful
                observer.schedule(event_handler, path=os.environ['PortalDestination'], recursive=True)
                print("Reconnection successful.")
                break
            except pika.exceptions.AMQPConnectionError:
                # Handle connection error and retry
                print("Failed to reconnect. Retrying in 5 seconds...")
                time.sleep(5)

    class FileHandler(FileSystemEventHandler):
        def __init__(self):
            self.processed_files = set()  # Set to store processed file paths

        def on_created(self, event):
            if event.is_directory:
                return

            file_path = event.src_path
            if file_path.endswith('.zip') and file_path not in self.processed_files:
                self.processed_files.add(file_path)
                time.sleep(5)
                process_file(file_path)


    # Create a connection to RabbitMQ
    # creds = pika.PlainCredentials('guest', 'guest')
    queue_name = 'file_progress_queue'
    # connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_SERVER'], 5672, '/', creds, heartbeat=60*10))
    connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_SERVER']))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.queue_purge(queue=queue_name)

    channel.basic_qos(prefetch_count=1)
    # Set up a file system watcher for the 'waiting_room' directory
    observer = Observer()
    event_handler = FileHandler()
    observer.schedule(event_handler, path=os.environ['PortalDestination'], recursive=True)
    observer.start()

    try:
        print(f"Watching '{os.environ['PortalDestination']}' directory for new files...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        connection.close()
    observer.join()

if __name__ == "__main__":
    send_file_to_queue()  # Start watching the 'waiting_room' directory
