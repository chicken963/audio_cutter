#!/bin/sh

docker-compose up
docker network create kafka-python
docker build -t audio-cutter .
docker run -p 4000:80 --name butcher --network kafka-python audio-cutter
docker network connect kafka-python "$KAFKA_CONTAINER_NAME"
docker network connect kafka-python "$ZOOKEEPER_CONTAINER_NAME"