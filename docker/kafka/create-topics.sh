#!/usr/bin/env bash
# Creates the topics with explicit partitions and retention (auto-creation is disabled).
# Replication factor 1 because the dev stack has a single broker.
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

create_topic() {
  local name="$1" retention_ms="$2"
  echo "Creating topic '${name}'"
  "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP_SERVER}" --create --if-not-exists \
    --topic "${name}" --partitions 3 --replication-factor 1 \
    --config cleanup.policy=delete --config "retention.ms=${retention_ms}"
}

create_topic payment-events 604800000            # 7 days
create_topic notification-events 604800000       # 7 days
create_topic notification-events-dlt 2592000000  # 30 days: needs human inspection

"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP_SERVER}" --list
