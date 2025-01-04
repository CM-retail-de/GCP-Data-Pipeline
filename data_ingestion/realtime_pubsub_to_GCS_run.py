import os
from google.cloud import pubsub_v1, storage
from datetime import datetime

# Configuration
PROJECT_ID = "xenon-aspect-444204-a3"
SUBSCRIPTION_NAME = "test-sub"
BUCKET_NAME = "my-first-gcs-bucket-chandra-name"  # Ensure no leading or trailing spaces
FILE_PREFIX = "pubsub_data"

# Initialize clients
subscriber = pubsub_v1.SubscriberClient()
storage_client = storage.Client()

def process_message_and_upload(data):
    """
    Process message data and upload it to GCS.
    """
    try:
        # Get current date
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_name = f"{FILE_PREFIX}_{current_date}.txt"
        local_file_path = f"/tmp/{file_name}"
        blob_name = file_name

        # Write data to a local file
        with open(local_file_path, "a") as file:
            file.write(data + "\n")

        print(f"Message written to local file: {local_file_path}")

        # Upload the file to GCS bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_file_path)

        print(f"File uploaded to GCS: gs://{BUCKET_NAME}/{blob_name}")

        # Clean up the local file
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Temporary file {local_file_path} deleted.")
    except Exception as e:
        print(f"Failed to process message or upload file to GCS: {e}")

def main():
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    print(f"Fetching messages from {subscription_path}...")

    # Use synchronous pull to fetch messages
    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 10}
    )

    for received_message in response.received_messages:
        message_data = received_message.message.data.decode("utf-8")
        print(f"Received message: {message_data}")

        # Process the message and upload to GCS
        process_message_and_upload(message_data)

        # Acknowledge the message
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": [received_message.ack_id]}
        )

    print("Finished processing messages.")

if __name__ == "__main__":
    main()
