# Kafka-wikimedia-stream (in development)

This project demonstrates how to consume events from the Wikimedia recent changes stream using Python, `aiohttp` for asynchronous HTTP requests, and `confluent-kafka` for producing messages to Kafka.

## Requirements

- Python 3.11.5
- `aiohttp` library (`pip install aiohttp`)
- `confluent-kafka` library (`pip install confluent-kafka`)
- Docker Engine
- Docker Compose

## Installation

1. Clone the repository:

    `git clone https://github.com/mainakm7/kafka_wikimedia_stream.git`


2. Install dependencies:

    `pip install -r requirements.txt`


3. Set up Kafka, Zookeeper, and PostgreSQL using Docker Compose:

    Ensure Docker Engine and Docker Compose are installed and running.
    run: `docker-compose up -d`

    Disclaimer: Using the Conduktor platform UI to monitor kafka topics. Present in docker compose file

## Configuration

- Update bootstrap.servers in main.py to localhost:9092 for Kafka connection.
- Update `topic_name` in `main.py` to the desired Kafka topic name.

## Usage

1. Run the main script to start consuming Wikimedia events:

###still in production


2. Logs will be outputted to `wiki_producer.log` in the current directory.

## Features

- **Asynchronous Event Handling**: Uses `aiohttp` for asynchronous HTTP requests to fetch events.
- **Kafka Integration**: Produces fetched events to a Kafka topic using `confluent-kafka`.
- **Error Handling**: Logs errors and exceptions to `wiki_producer.log` for troubleshooting.

## Contributing

Contributions are welcome! Please fork the repository and submit pull requests with improvements or new features.

## License

This project is licensed under the MIT License - see the LICENSE file for details.


