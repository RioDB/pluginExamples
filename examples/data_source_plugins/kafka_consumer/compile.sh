#!/bin/sh
mvn clean compile assembly:single
cp target/kafka_consumer.jar .

