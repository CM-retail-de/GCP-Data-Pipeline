from google.cloud import  storage
from datetime import datetime

gcs_client = storage.Client("xenon-aspect-444204-a3")

bucket = gcs_client.bucket("test-bucket311123")
bucket.storage_class = "STANDARD"


gcs_client.create_bucket(bucket,location="US")
