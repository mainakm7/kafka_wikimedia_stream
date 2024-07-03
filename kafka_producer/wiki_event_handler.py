from confluent_kafka import Producer, KafkaException, CompressionType
import logging
import asyncio

logging.basicConfig(level=logging.INFO, filename="wiki_producer.log", filemode="w")
log = logging.getLogger(name=__name__)


def delivery_report(err, msg):
    if err:
        log.error(f"Failed to deliver message: {err}")
    else:
        log.info(
            f"Message metadata:\n"
            f"Topic: {msg.topic()}\n"
            f"Partition: [{msg.partition()}] \n"
            f"Offset: {msg.offset()} \n"
            f"Timestamp: {msg.timestamp()} \n"
            f"Msg Key: {msg.key().decode('utf-8') if msg.key() else 'None'}"
        )

class WikimediaEventHandler:
    def __init__(self, producer: Producer, topic):
        self.producer = producer
        self.topic = topic
    
    def on_close(self):
        self.producer.flush()
    
    async def on_message(self, msg):
        log.info(f"Msg received: {msg}")
        try:
            self.producer.produce(self.topic, value=msg.encode("utf-8"), compression=CompressionType.SNAPPY, callback=delivery_report)
            log.info(f"Msg passed {msg}")
        except KafkaException as e:
            log.error(f"Kafka exception received: {e}")
        except Exception as e:
            log.error(f"Exception received: {e}")