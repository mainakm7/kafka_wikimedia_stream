from confluent_kafka import Producer, KafkaException
from producer_utils import delivery_report
import logging
import asyncio

logging.basicConfig(level=logging.INFO, filename="wiki_producer.log", filemode="w")
log = logging.getLogger(name=__name__)

class WikimediaEventHandler:
    def __init__(self, producer: Producer, topic):
        self.producer = producer
        self.topic = topic
    
    def on_close(self):
        self.producer.flush()
    
    async def on_message(self, msg):
        log.info(f"Msg received: {msg}")
        try:
            self.producer.produce(self.topic, value=msg.encode("utf-8"), callback=delivery_report)
            log.info(f"Msg passed {msg}")
        except KafkaException as e:
            log.error(f"Kafka exception received: {e}")
        except Exception as e:
            log.error(f"Exception received: {e}")