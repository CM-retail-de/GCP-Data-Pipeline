import os 
from google.cloud import pubsub_v1, storage
from datetime import datetime

#Configuration

PROJECT_ID = "xenon-aspect-444204-a3"
SUBSCRIPTION_NAME ="projects/xenon-aspect-444204-a3/subscriptions/test-sub"
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


def main():
    #Subscribe to the Pub/Sub subscription
    subscription_path = susbcriber.subscription_path(PROJECT_ID,SUBSCRIPTION_NAME)
    print(f"Listening for message on {subscription_path}...")

    #Use a streaming pull for contineous listening

    streaming_pull_feature = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull_feature.result() #keep the subscriber listening
    except keyboardInterrupt:
        streaming_pull_feature.cancel() #Stop listening on interrrupt
        upload_to_GCS()  #Ensure file upload on termination


if __name__ = "__main__":
    main()




