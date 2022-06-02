"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer,CachedSchemaRegistryClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger()
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

BROKER_URL = 'PLAINTEXT://localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            'bootstrap.servers':BROKER_URL,
            'schema.registry.url': SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)


        schema_registry = CachedSchemaRegistryClient({
            'url':SCHEMA_REGISTRY_URL
        })
        self.producer = AvroProducer(
           self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #

        # the Kafka Broker.
        client = AdminClient({'bootstrap.servers':BROKER_URL})
        topic_metadata = client.list_topics()

        logger.info(f'Checking if a {self.topic_name} exist ...')
        if (self.topic_exists(client,self.topic_name)) is False:
            logger.info(f'Topic ({self.topic_name}) deosn\'t exist, Creating one ...')

            """Creates the topic with the given topic name"""
            futures = client.create_topics(
                [
                    NewTopic(
                        topic=self.topic_name,
                        num_partitions=self.num_partitions,
                        replication_factor=self.num_replicas,
                        # config={
                        #     "cleanup.policy": "delete",
                        #     "compression.type": "lz4",
                        #     "delete.retention.ms": "2000",
                        #     "file.delete.delay.ms": "2000",
                        # },
                    )
                ]
            )

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info("topic created")
                except Exception as e:
                    logger.error(f"failed to create topic {self.topic_name}: {e}")
        else:
            logger.info(f'{self.topic_name} topic already exist')


    def topic_exists(self,client, topic_name):
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))


    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info("producer close successfully.")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))



if __name__ == "__main__":
    a = Producer(topic_name='test1',key_schema='username')