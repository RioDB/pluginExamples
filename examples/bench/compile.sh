#!/bin/sh
mvn clean compile assembly:single
cp target/bench.jar .

