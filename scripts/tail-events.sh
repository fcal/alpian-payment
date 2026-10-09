#!/usr/bin/env bash
#
# Prints the records on an event topic, with each protobuf value decoded.
#
#   scripts/tail-events.sh             # every record from the beginning, then exit
#   scripts/tail-events.sh -f          # keep following new records (Ctrl-C to stop)
#   scripts/tail-events.sh -t TOPIC    # another topic (default: payment-events)
#
# The message type comes from the event-type header. Requires kcat and protoc
# (brew install kcat protobuf). BOOTSTRAP_SERVER defaults to localhost:9092.
#
# Each value is fetched separately: raw protobuf has no length prefix to split a stream on.

set -euo pipefail

bootstrap="${BOOTSTRAP_SERVER:-localhost:9092}"
topic="payment-events"
follow=false

while getopts "ft:h" opt; do
  case "$opt" in
    f) follow=true ;;
    t) topic="$OPTARG" ;;
    *) sed -n '3,7p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
  esac
done

for tool in kcat protoc; do
  if ! command -v "$tool" >/dev/null; then
    echo "error: $tool not found (brew install kcat protobuf)" >&2
    exit 1
  fi
done

proto_root="$(cd "$(dirname "$0")/.." && pwd)/proto/src/main/proto"
proto_file="alpian/payment/v1/payment_events.proto"
default_message="alpian.payment.v1.PaymentEvent"

# -e: exit at the end of the topic; -u: unbuffered, for following.
mode=-e
$follow && mode=-u

kcat -b "$bootstrap" -t "$topic" -C -o beginning -q "$mode" \
  -f '%p\t%o\t%k\t%h\t%T\n' |
  while IFS=$'\t' read -r partition offset key headers timestamp; do
    echo "--- partition=$partition offset=$offset key=$key timestamp=$timestamp"
    echo "    headers: $headers"
    message="$(sed -n 's/.*event-type=\([^,]*\).*/\1/p' <<<"$headers")"
    value="$(kcat -b "$bootstrap" -t "$topic" -C -p "$partition" -o "$offset" -c 1 -e -q \
      -D '' -f '%s' | protoc --decode="${message:-$default_message}" -I "$proto_root" \
      "$proto_file" 2>/dev/null)" || value="(value does not decode as ${message:-$default_message})"
    sed 's/^/    /' <<<"$value"
  done
