PROJECT_ID=modified-hull-472911-t6
REGION=europe-central2
CLUSTER=my-cluster
BUCKET=kafka-streaming-bucket-1
JOB_FILE=bronze_to_silver.py
GCS_JOB_PATH=gs://$(BUCKET)/code

# Create Dataproc cluster (single node for testing)
cluster-create:
	gcloud dataproc clusters create $(CLUSTER) \
		--region=$(REGION) \
		--single-node \
		--project=$(PROJECT_ID)

# Delete Dataproc cluster
cluster-delete:
	gcloud dataproc clusters delete $(CLUSTER) \
		--region=$(REGION) \
		--quiet

# Upload job to GCS
upload:
	gsutil cp $(JOB_FILE) $(GCS_JOB_PATH)

# Submit job to cluster
submit: upload
	gcloud scheduler jobs create pubsub bronze-to-silver-job \
	  --schedule="* * * * *" \
	  --topic=dataproc-submit-topic \
	  --message-body='{\
		"type":"dataproc_job",\
		"pyspark_file":"gs://kafka-streaming-bucket-1/code/bronze_to_silver.py",\
		"cluster":"my-cluster",\
		"region":"europe-central2",\
		"project":"modified-hull-472911-t6"\
	  }' \
	  --time-zone="Europe/Warsaw" \
	  --location=europe-west1






# Run job in serverless mode (no cluster needed)
submit-serverless: upload
	gcloud dataproc batches submit pyspark $(GCS_JOB_PATH) \
		--region=$(REGION) \
		--project=$(PROJECT_ID)

# Clean bucket job files
clean:
	gsutil rm -f $(GCS_JOB_PATH)
