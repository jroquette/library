"""
Kafka Consumer
"""
import json

from confluent_kafka import Consumer

from app_name.api.services import purchase_process


class KafkaConsumer:
    """
    Kafka Consumer class
    """

    def __init__(self, group_id, offset, topics, kafka_broker="localhost:9092", config=None):
        """
        Kafka consumer's constructor

        :param group_id:
        :param offset:
        :param topics:
        :param kafka_broker:
        :param config:
        """
        self.group_id = group_id
        self.offset = offset
        self.topics = topics
        self.kafka_broker = kafka_broker
        if config is None:
            config = self.properties
        self.consumer = Consumer(config)

    @property
    def properties(self):
        """
        Properties of consumer

        :return: dictionary with config to consumer
        """
        properties = {
            "bootstrap.servers": self.kafka_broker,
            "group.id": self.group_id,
            "auto.offset.reset": self.offset,
        }
        return properties

    def subscribe(self, topics):
        """
        Subscribe in topics

        :param topics: list of topics
        """
        self.consumer.subscribe(topics)

    def close(self):
        """
        Close application
        """
        self.consumer.close()

    def run(self):
        """
        Start application
        """
        self.subscribe(self.topics)
        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            if msg.topic() == "PURCHASE_BOOK":
                print(
                    f'Received key {msg.key().decode("utf-8")} message: {msg.value().decode("utf-8")}'
                )
                purchase_data = json.loads(msg.value())
                purchase_process.purchase_process(purchase_data)

        self.close()


if __name__ == "__main__":
    consumer = KafkaConsumer(group_id=0, offset="earliest", topics=["PURCHASE_BOOK"])
    consumer.run()
