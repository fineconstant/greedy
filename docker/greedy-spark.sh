#!/usr/bin/env bash

echo "Building jar..."
mvn clean package
echo "Creating Docker image..."
mvn docker:build

echo "Starting services..."
cd docker/greedy-dev/
docker-compose up