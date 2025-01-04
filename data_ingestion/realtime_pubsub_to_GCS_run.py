import os 
from google.cloud import pubsub_v1, storage
from datetime import datetime

#Configuration test

PROJECT_ID = "xenon-aspect-444204-a3"
SUBSCRIPTION_NAME ="test-sub"
BUCKET_NAME = " my-first-gcs-bucket-chandra-name"
FILE_PREFIX = "pubsub_data"


#INITIALIZE CLIENTS
subscriber = pubsub_v1.SubscriberClient()
storage_client = storage.Client()

def callback(message):
    """
    Callback function to process pubsub message
    """
    try:
        #Get current date
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_name = f"{FILE_PREFIX}_{current_date}.txt"
        local_file_path = f"/tmp/{file_name}"
        blob_name = f"{file_name}"

        #Upload to GCS bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = blob.bucket(blob_name)
        blob.upload_from_filename(local_file_path)

        print(f"file uploaded to GCS: gs://{BUCKET_NAME}/{blob_name} ")


        #Clean up local file
        os.remove(local_file_path)
    except Exception as e:
        print(f"failed to upload file to GCS: {e}")

import time

def main():
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    print(f"Fetching messages from {subscription_path}...")

    # Use synchronous pull
    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 10}
    )

    for received_message in response.received_messages:
        print(f"Received message: {received_message.message.data.decode('utf-8')}")
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": [received_message.ack_id]}
        )

    print("Finished processing messages.")

if __name__ == "__main__":
    main()

#Test1
#Test2



