#!/usr/bin/env bash
echo 'building is starting...'
cd kafka/
mvn clean install deploy
cd ../kafka-integration-spring/
mvn clean install deploy