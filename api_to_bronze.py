import json
# import apache_beam as beam
# from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from google.cloud import storage
from google.api_core.exceptions import Forbidden



def read_config():
    # reads the client configuration from client.properties
    # and returns it as a key-value map
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config


def consume(topic, config,bucket):
    sr_conf = {
        "url": config["schema.registry.url"],
        "basic.auth.user.info": config["basic.auth.user.info"]
    }
    sr = SchemaRegistryClient(sr_conf)
    json_deserializer = JSONDeserializer(
        schema_str=None,
        schema_registry_client=sr
    )

    consumer_conf = {
        "bootstrap.servers": config["bootstrap.servers"],
        "security.protocol": config["security.protocol"],
        "sasl.mechanisms": config["sasl.mechanisms"],
        "sasl.username": config["sasl.username"],
        "sasl.password": config["sasl.password"],
        "group.id": "python-jsonsr-consumer",
        "auto.offset.reset": "earliest",
        "value.deserializer": json_deserializer,
        "key.deserializer": lambda k, ctx: k.decode("utf-8") if k else None,
    }

    # sets the consumer group ID and offset


    config["group.id"] = "python-group-2"
    config["auto.offset.reset"] = "earliest"

    # creates a new consumer instance
    consumer = DeserializingConsumer(consumer_conf)

    # subscribes to the specified topic
    consumer.subscribe([topic])
    result = []
    minute,hour,day,month,year =datetime.now().minute,datetime.now().hour,datetime.now().day,datetime.now().month,datetime.now().year
    try:
        while True:
            # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(timeout=3)
            if msg is not None and msg.error() is None:
                try:
                    value = json.loads(msg.value()["result"])
                except:
                    print("Error in response, likely due to rate limit reached")
                    continue
                if result and value["LastBlock"]==result[-1]["LastBlock"]:
                    continue
                take_time=datetime.now()
                if not (take_time.year==year    and
                       take_time.month==month   and
                       take_time.day==day       and
                       take_time.hour==hour     and
                       take_time.minute==minute
                ):
                    try:
                        blob = bucket.blob(f"bronze/{year}/"
                                           f"{month}/"
                                           f"{day}/"
                                           f"{hour}/"
                                           f"{minute}"
                                           ".json"
                        )
                        print("Sent to storage:")
                        print(f"bronze/{year}/"
                              f"{month}/"
                              f"{day}/"
                              f"{hour}/"
                              f"{minute}"
                              )
                        blob.upload_from_string(json.dumps(result), content_type="application/json")
                    except Forbidden:  # time overflow all records go to next minute
                        print("Forbidden")
                        minute, hour, day, month, year = datetime.now().minute, datetime.now().hour, datetime.now().day, datetime.now().month, datetime.now().year
                        continue

                # Upload plain text directly
                    result=[]
                print(f"adding value:{value}")
                result.append(value)

    except KeyboardInterrupt:
        pass
    finally:
        # closes the consumer connection
        consumer.close()


def main():
    client = storage.Client.from_service_account_json("service_account.json")
    bucket = client.bucket("kafka-streaming-bucket-1")
    config = read_config()
    topic = "gas"

    consume(topic, config,bucket)

if __name__=="__main__":
    main()
