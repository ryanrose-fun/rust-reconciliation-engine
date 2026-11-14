#!/usr/bin/env bash
# Isolated development broker. Run in WSL; keep this terminal open.
set -euo pipefail
command -v java >/dev/null || { echo 'Install Java 17+ first (e.g. sudo apt install openjdk-21-jre-headless).' >&2; exit 1; }
root="${KAFKA_LAB_HOME:-$HOME/.local/share/rust-gpu-finance/kafka}"
mkdir -p "$root"
cd "$root"
version=4.0.1
archive="kafka_2.13-$version.tgz"
expected=23b241098672d828ca86a6dd1dd99102fcdb9de8680ecf213b47251a7ae7a342343cf41adf6fee9f920d171ae13f94c39475165f5a246e1a46609a7d9d5a6811
if [ ! -f "$archive" ]; then
  curl -fL --retry 2 "https://archive.apache.org/dist/kafka/$version/$archive" -o "$archive"
fi
printf '%s  %s\n' "$expected" "$archive" | sha512sum -c -
if [ ! -d "kafka_2.13-$version" ]; then tar -xzf "$archive"; fi
broker_ip="${KAFKA_LAB_IP:-$(hostname -I | awk '{print $1}')}"
case "$broker_ip" in ''|*[!0-9.]*) echo 'KAFKA_LAB_IP must be an IPv4 address on this WSL host' >&2; exit 1;; esac
cat > server.properties <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:19093
listeners=PLAINTEXT://$broker_ip:19092,CONTROLLER://127.0.0.1:19093
advertised.listeners=PLAINTEXT://$broker_ip:19092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$root/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
log.retention.hours=24
EOF
if [ ! -f data/meta.properties ]; then
  cluster_id="$("kafka_2.13-$version/bin/kafka-storage.sh" random-uuid)"
  "kafka_2.13-$version/bin/kafka-storage.sh" format --standalone -t "$cluster_id" -c server.properties
fi
printf '\nLocal test broker: %s:19092\nStorage: %s/data\nCtrl+C stops Kafka.\n\n' "$broker_ip" "$root"
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}"
exec "kafka_2.13-$version/bin/kafka-server-start.sh" server.properties
