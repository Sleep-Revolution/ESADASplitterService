print("Fuk", flush=True)
import pika
import json
import os
import src.noxmultinight as SplitterService
import datetime
import uuid
import requests
import shutil

class STATUS_MESSAGES:
    FAIL = -1
    STARTED = 0
    FINISHED = 1 
    JOBEND = 2
    WARN = 3

class ProgressMessage:
    def __init__(self, stepNumber:int, taskTitle:str, progress:int, message:str="", fileName: str="", uploadId:int=None, datasetName:str=None):
        self.StepNumber = stepNumber
        self.TaskTitle = taskTitle
        self.Progress = progress
        self.FileName = fileName
        self.Message = message
        self.UploadID = uploadId
        self.DatasetName = datasetName
    def serialise(self) -> dict: 
        return {
            'UploadId': self.UploadID,
            'StepNumber': self.StepNumber,
            'TaskTitle': self.TaskTitle,
            'Progress': self.Progress,
            'Message': self.Message,
            'DatasetName': self.DatasetName
        }
    # UploadId: str
    # StepNumber: int
    # TaskTitle: str
    # Progress: int
    # Message: str
    # DatasetName: str


creds = pika.PlainCredentials('server', 'server')

#logger = logging.getLogger("consumer")

def process_file(channel,message): 
    print(message)
    path_to_zip = os.path.join(os.environ['PORTAL_DESTINATION_FOLDER'], message['path'], message['name'])

    projectName = str(uuid.uuid4())
    projectLocation = os.path.join('temp_uuids', projectName)
    os.makedirs(projectLocation)   


    centreId = message['centreId']
    isDataset = message['dataset']
    uploadId = message['uploadId']
    path = message['path'] #centre name
    datasetName = '' if not isDataset else path

    if not os.path.exists(path_to_zip):
        raise Exception(f"Got a message for a file for centre bla, but no folder exists in portal waiting room ({path_to_zip}).")


    def basicpublish(status=-2, message=""):
        url = f"{os.environ['FRONT_END_SERVER']}/meta/log_upload"
        entry = ProgressMessage(step, task, status, message, name, uploadId, datasetName=datasetName)
        print(entry.serialise())
        r = requests.post(url, json=entry.serialise())


    print("Starting night splitting process")


    name = message["name"]
    esr = name[:-4]
    step = 0 
    task = 'Split upload'
    basicpublish(status=STATUS_MESSAGES.STARTED)
    Success, Message, Name = SplitterService.NoxSplitting(path_to_zip, esr, projectLocation)

    for subdir in os.listdir(projectLocation):
        print(subdir)
        nightNumber = int(subdir[-2:])

        destination = os.path.join(os.environ['INDIVIDUAL_NIGHT_WAITING_ROOM'], path, subdir)
        # move the subdir to the Individual night waiting room
        if os.path.isdir(destination):
            shutil.rmtree(destination)
        shutil.copytree(os.path.join(projectLocation,subdir), destination)
        # notify the front end.
        # uploadId is used to connnect the night to a specific upload.
        requests.post(f"{os.environ['FRONT_END_SERVER']}/add-night-to-upload/{uploadId}/{nightNumber}")
        shutil.rmtree(os.path.join(projectLocation,subdir))
        # @app.post("/add-night-to-upload/{uploadId}/{nightNumber}")


    basicpublish(status=STATUS_MESSAGES.FINISHED, message=f"Recording was split into {len(os.listdir(projectLocation))}")
    print("Ending night splitting process")

    # delete the project location. 
    shutil.rmtree(os.path.join(projectLocation))
        
    return Success, Message, Name







def callback(ch, method, properties, body):
    # logger.info(f"Received message: {body}")
    # try:
    message = json.loads(body)
    # {"name": "BJEMSLEV0316.zip", "path": "EmilSRE", "dataset": false, "centreId": 3, "uploadId": 82}

    print(f"Received file: {message}")
    
    time = datetime.datetime.now()
    message["Time"] = time.isoformat()

    Success, Message, Name = process_file(ch,message)
    time = datetime.datetime.now()
    
    message["Time"] = time.isoformat()
    if Success:
        print("Processing completed")
    else:
        print(f"Processing failed: {Message}")

        # Acknowledge the message using the delivery tag
        
    # except Exception as e:
        # Handle any exceptions that occur during processing
        # print(f"Error processing message: {str(e)}")



    ch.basic_ack(delivery_tag=method.delivery_tag)

    print("Waiting for new files. To exit, press CTRL+C")

    

if __name__ == '__main__':
    print("Yahoo?", flush=True)
    connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_SERVER'], 5672, '/', creds, heartbeat=60*10))
    channel = connection.channel()

    channel.queue_declare(queue=os.environ['SPLITTER_QUEUE_NAME'], durable=True)
    # channel.queue_purge(queue=queue_name)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=os.environ['SPLITTER_QUEUE_NAME'], on_message_callback=callback)

    print("Waiting for files. To exit, press CTRL+C", flush=True)
    channel.start_consuming()

