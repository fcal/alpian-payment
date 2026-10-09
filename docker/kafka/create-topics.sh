#!/usr/bin/env bash
# Creates the topics the services expect, with explicit partition counts and retention.
#
# Declared here rather than relying on broker auto-creation (disabled in compose.yaml) or on
# Spring's NewTopic beans: auto-created topics silently take broker defaults, and a topic's
# partition count cannot be reduced later. Making it explicit keeps the two services from
# disagreeing about the contract.
#
# Partition counts are 3 to exercise real partitioning locally; replication factor is 1
# because the dev stack is a single broker. A real deployment would use RF >= 3 with
# min.insync.replicas = 2.
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

create_topic() {
  local name="$1" partitions="$2"
  shift 2
  local configs=()
  for config in "$@"; do
    configs+=(--config "$config")
  done

  echo "Creating topic '${name}' (${partitions} partitions)"
  "${KAFKA_TOPICS}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    "${configs[@]}"
}

# Payment lifecycle events, published by the payment service's outbox relay and keyed by
# account id. These are immutable facts, not state, so the policy is `delete` with bounded
# retention -- NOT `compact`. Compaction retains the latest record per key; with a key that
# is effectively unique per event it would retain everything forever.
create_topic payment-events 3 \
  "cleanup.policy=delete" \
  "retention.ms=604800000"   # 7 days

# Notifications emitted to the sender after deduplication.
create_topic notification-events 3 \
  "cleanup.policy=delete" \
  "retention.ms=604800000"

# Dead-letter topics. Longer retention: these need human inspection, so they must outlive the
# incident that produced them.
#
# Payment events the notification service cannot turn into a notification: undecodable, invalid,
# or keyed by something other than their account id.
create_topic payment-events-dlt 3 \
  "cleanup.policy=delete" \
  "retention.ms=2592000000"  # 30 days

# Notifications whose delivery failed every retry.
create_topic notification-events-dlt 3 \
  "cleanup.policy=delete" \
  "retention.ms=2592000000"

echo "Topics present:"
"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP_SERVER}" --list
