from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


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


def consume(topic, config):
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

    try:
        while True:
            # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                value = msg.value()
                print(value)
    except KeyboardInterrupt:
        pass
    finally:
        # closes the consumer connection
        consumer.close()


def main():
    config = read_config()
    topic = "gas"

    consume(topic, config)


main()
