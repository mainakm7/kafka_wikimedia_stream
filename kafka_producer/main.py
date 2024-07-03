from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from producer_utils import create_topic, consume_wikimedia_events
from wiki_event_handler import WikimediaEventHandler
import logging
import asyncio

logging.basicConfig(level=logging.INFO, filename="wiki_producer.log", filemode="w")
log = logging.getLogger(name=__name__)

def main():
    conf = {
        "bootstrap.servers": "127.0.0.1:9092",
        "client.id": "python-producer",
        "acks": "all",
        "enable.idempotence": True
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

    try:
        asyncio.run(consume_wikimedia_events(url, event_handler))
    except Exception as e:
        log.error(f"Error consuming events: {e}")
    finally:
        event_handler.on_close()

if __name__ == "__main__":
    main()