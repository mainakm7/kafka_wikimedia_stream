from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import NewTopic, AdminClient
import logging
import aiohttp
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

def create_topic(admin_client, topic_name, num_partitions=3, replication_factor=1):
    """Create a topic if it doesn't exist"""
    topic_list = admin_client.list_topics(timeout=10).topics
    if topic_name in topic_list:
        log.info(f"Topic {topic_name} already exists")
    else:
        log.info(f"Creating topic {topic_name}")
        new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result() 
                log.info(f"Topic {topic} created")
            except Exception as e:
                log.error(f"Failed to create topic {topic}: {e}")
                raise

class WikimediaEventHandler:
    def __init__(self, producer, topic):
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

async def consume_wikimedia_events(url, event_handler):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            try:
                while True:
                    line = await response.content.readline()
                    if not line:
                        break
                    await event_handler.on_message(line.decode('utf-8').strip())
            except KeyboardInterrupt as e:
                log.error(f"Stream break: {e}")

def main():
    conf = {
        "bootstrap.servers": "127.0.0.1:9092",
        "client.id": "python-producer"
    }

    admin_client = AdminClient(conf)
    topic_name = "wikimedia_report"

    try:
        create_topic(admin_client, topic_name)
    except Exception as e:
        log.error(f"Failed to create topic: {e}")
        return

    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    producer = Producer(conf)
    event_handler = WikimediaEventHandler(producer, topic_name)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(consume_wikimedia_events(url, event_handler))
    except Exception as e:
        log.error(f"Error consuming events: {e}")
    finally:
        event_handler.on_close()

if __name__ == "__main__":
    main()
