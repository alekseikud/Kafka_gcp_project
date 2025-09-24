from google.cloud import storage
from google.api_core.exceptions import Forbidden,NotFound
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
# import apache_beam as beam
# from apache_beam.options.pipeline_options import PipelineOptions


if __name__ == "__main__":
    if os.path.exists("service_account.json"):
        storage_client= storage.Client.from_service_account_json("service_account.json")
    else:
        storage_client= storage.Client()  # use Dataproc default service account
    bucket=storage_client.bucket("kafka-streaming-bucket-1")
    try:
        bookmark=json.loads(bucket.blob("bookmarks/last_bookmark_bronze_to_silver").download_as_text())
        print(bookmark)
    except NotFound:
        bookmark={
            "minute":0,
            "hour":0,
            "day":0,
            "month":0,
            "year":0
        }
    minute=int(bookmark["minute"])
    hour=int(bookmark["hour"])
    day=int(bookmark["day"])
    month=int(bookmark["month"])
    year=int(bookmark["year"])
    resulting_list=[]
    max_seen = {"year": year, "month": month, "day": day, "hour": hour, "minute": minute}
    for blob in bucket.list_blobs(prefix="bronze/"):
        name=blob.name[0:-5].split("/")

        blob_year=int(name[1])
        blob_month=int(name[2])
        blob_day=int(name[3])
        blob_hour=int(name[4])
        blob_minute=int(name[5])

        if (blob_year, blob_month, blob_day, blob_hour, blob_minute) > (year, month, day, hour, minute):
            resulting_list.append(blob.name)
            if (blob_year, blob_month, blob_day, blob_hour, blob_minute) > (
                    max_seen["year"], max_seen["month"], max_seen["day"], max_seen["hour"], max_seen["minute"]
            ):
                max_seen = {
                    "year": blob_year,
                    "month": blob_month,
                    "day": blob_day,
                    "hour": blob_hour,
                    "minute": blob_minute,
                }


    if resulting_list:
        spark = SparkSession.builder.appName("BronzeToSilverMicrobatch").getOrCreate()
        print(["gs://kafka-streaming-bucket-1/"+record for record in resulting_list])
        df=spark.read.json(["gs://kafka-streaming-bucket-1/"+record for record in resulting_list])
        df.write.mode("append").parquet(f"gs://kafka-streaming-bucket-1/silver/{datetime.today().strftime('%Y-%m-%d')}.parquet")
    print(resulting_list)
    for name in resulting_list:
        print(bucket.blob(name).download_as_text())

    bucket.blob("bookmarks/last_bookmark_bronze_to_silver").upload_from_string(json.dumps(max_seen))
