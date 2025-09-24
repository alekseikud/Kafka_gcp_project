FROM python:3.12-slim
WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY api_to_bronze.py /app/api_to_bronze.py
COPY client.properties /app/client.properties
COPY service_account.json /app/service_account.json
CMD ["python","api_to_bronze.py"]
