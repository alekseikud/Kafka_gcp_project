.PHONY: start_script

start_script:
	gcloud compute instances create-with-container api-to-bronze-vm \
  --zone=europe-central2-b \
  --container-image=docker.io/kudelich/api-to-bronze:amd64 \
  --machine-type=e2-small
