import os
import pika
import json
import time
import src.watchdog as doggo
import src.consumer as consumer
import threading


if __name__ == "__main__":
    consume_thread = threading.Thread(target=consumer.main)
    consume_thread.start()
    doggo_thread = threading.Thread(target=doggo.send_file_to_queue)
    doggo_thread.start()  # Start watching the 'waiting_room' directory
