#!/bin/sh
mvn clean compile assembly:single
cp target/http.jar .

