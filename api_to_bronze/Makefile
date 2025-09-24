.PHONY: start_script

start_script: build_image,create_vm_with_cntainer,check
	

build_image:
	gcloud builds submit . \
  --tag europe-central2-docker.pkg.dev/modified-hull-472911-t6/my-repo/api-to-bronze:amd64

create_vm_with_cntainer:
	gcloud compute instances create-with-container api-to-bronze-vm \
  --zone=europe-central2-b \
  --machine-type=e2-small \
  --container-image=europe-central2-docker.pkg.dev/modified-hull-472911-t6/my-repo/api-to-bronze:amd64 \
  --restart-on-failure

check:
	@echo If you see here new vm than everything ran successfully &&\
	gcloud compute instances list
