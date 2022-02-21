"""
Kafka Producer to labels
"""
import json
from uuid import uuid4
from confluent_kafka import Producer


# Producer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md


class BadRequest(Exception):
    pass


class KafkaProducer:
    """
    Kafka Producer Generic
    """

    def __init__(self, config=None, broker_kafka="localhost:9092"):
        """
        Constructor of Kafka Producer
        """
        self.broker_kafka = broker_kafka
        if config is None:
            config = self.properties
        self.producer = Producer(config)

    @property
    def properties(self):
        """
        Properties to Kafka Producer
        """
        properties = {
            "bootstrap.servers": self.broker_kafka,
        }
        return properties

    @staticmethod
    def delivery_callback(err, msg):
        """
        Print errors if happen when send a message to kafka

        :param err: Error
        :param msg: Message error
        """
        if err:
            print("%% Message failed delivery: %s\n" % err)
        else:
            print(
                "%% Message delivered to %s [%d] @ %d\n"
                % ({msg.topic()}, msg.partition(), msg.offset())
            )

    def send(self, topic, key=None, value=None):
        """
        Send a message to kafka server

        :param topic: Topic of Kafka
        :param key: Key of message
        :param value: message body
        """
        if key is None:
            key = str(uuid4())
        if not isinstance(value, str):
            value = self.serializer(value)
        try:
            self.producer.produce(topic=topic, value=value, key=key)
            self.producer.flush()
        except BufferError:
            raise BadRequest(
                "%% Local producer queue is full (%d messages awaiting delivery): try again\n"
                % len(self.producer)
            )

    @staticmethod
    def serializer(obj, encode="utf-8"):
        """
        Serializer object to string

        :param obj: object to serializer
        :param encode: encode
        :return: object in string
        """
        return json.dumps(obj).encode(encode)


producer = KafkaProducer()
