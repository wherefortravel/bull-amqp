#!/bin/bash
set -e

MAX_ATTEMPTS=30
ATTEMPT=1

# Get the RabbitMQ port from AMQP_URL or use default
AMQP_PORT="${AMQP_PORT:-5673}"
MANAGEMENT_PORT="${MANAGEMENT_PORT:-15673}"

echo "Waiting for RabbitMQ to be ready on ports $AMQP_PORT (AMQP) and $MANAGEMENT_PORT (Management)..."

while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
  # Check if management API is responding (more reliable than docker exec)
  if curl -s -f -u guest:guest "http://localhost:$MANAGEMENT_PORT/api/health/checks/alarms" > /dev/null 2>&1; then
    echo "RabbitMQ is ready!"
    exit 0
  fi

  echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: RabbitMQ not ready yet, waiting..."
  sleep 2
  ATTEMPT=$((ATTEMPT + 1))
done

echo "RabbitMQ failed to become ready after $MAX_ATTEMPTS attempts"
exit 1
