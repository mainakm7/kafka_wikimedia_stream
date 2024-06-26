from confluent_kafka.admin import NewTopic, AdminClient
from wiki_event_handler import WikimediaEventHandler
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

def create_topic(admin_client: AdminClient, topic_name, num_partitions=3, replication_factor=1):
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


async def consume_wikimedia_events(url, event_handler: WikimediaEventHandler):
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

