import os
import pika
import json
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging

def send_file_to_queue(file_path,channel):
    name = file_path.split('\\')[-1].split('.')[0]
    message = {
        "file_path_zip": file_path,
        "name": name
    }

    # Ensure that the message is published before returning
    channel.basic_publish(exchange='', routing_key=f"file_progress_queue", body=json.dumps(message))
    print(f"Sent from process: {message}")


class FileHandler(FileSystemEventHandler):
    def __init__(self, channel):
            self.channel = channel

    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        if file_path.endswith('.zip'):
            time.sleep(5)  # Simulate some processing time
            send_file_to_queue(file_path, self.channel)

