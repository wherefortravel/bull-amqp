#!/bin/bash
set -e

MAX_ATTEMPTS=30
ATTEMPT=1

echo "Waiting for RabbitMQ to be ready..."

while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
  if docker compose exec -T rabbitmq rabbitmq-diagnostics ping > /dev/null 2>&1; then
    echo "RabbitMQ is ready!"
    exit 0
  fi

  echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: RabbitMQ not ready yet, waiting..."
  sleep 2
  ATTEMPT=$((ATTEMPT + 1))
done

echo "RabbitMQ failed to become ready after $MAX_ATTEMPTS attempts"
exit 1
