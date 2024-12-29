from google.cloud import storage

def create_bucket(bucket_name, location="US", storage_class="STANDARD"):
    """
    Creates a new bucket in Google Cloud Storage.

    Args:
        bucket_name (str): The name of the bucket to create (must be unique globally).
        location (str): The location/region for the bucket (default: "US").
        storage_class (str): The storage class for the bucket (default: "STANDARD").
    
    Returns:
        Bucket: The created bucket object.
    """
    try:
        # Initialize a GCS client
        client = storage.Client()

        # Create the bucket
        bucket = client.bucket(bucket_name)
        bucket.storage_class = storage_class

        new_bucket = client.create_bucket(bucket, location=location)

        print(f"Bucket '{new_bucket.name}' created in '{new_bucket.location}' with storage class '{new_bucket.storage_class}'.")
        return new_bucket

    except Exception as e:
        print(f"Error creating bucket: {e}")
        return None

if __name__ == "__main__":
    # Replace with your desired bucket name (must be globally unique)
    BUCKET_NAME = "my-first-gcs-bucket-chandra-name"
    LOCATION = "US"  # Replace with a region like "asia-south1" if needed
    STORAGE_CLASS = "STANDARD"  # Options: "NEARLINE", "COLDLINE", "ARCHIVE", etc.

    create_bucket(BUCKET_NAME, location=LOCATION, storage_class=STORAGE_CLASS)
