#!/usr/bin/env bash

echo "building is starting.."
cd kafka-integration-spring && mvn clean install deploy -U
cd ../kafka-spring-boot-starter/ && mvn clean install deploy -U
cd ../kafka-tools && mvn clean install deploy -U
