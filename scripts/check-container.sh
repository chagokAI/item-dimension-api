#!/usr/bin/env bash
set -Eeuo pipefail
image=${1:?Usage: bash scripts/check-container.sh IMAGE}
name="chagok-dimension-check-${RANDOM}-$$"
cleanup() {
  local status=$?
  trap - EXIT
  if [ "$status" -ne 0 ]; then docker logs "$name-worker" >&2 2>/dev/null || true; fi
  docker rm -fv "$name-worker" "$name-kafka" >/dev/null 2>&1 || true
  docker network rm "$name" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

docker run --rm --network none --entrypoint python "$image" -c '
import app.worker, app.main, app.loader, app.healthcheck
from pathlib import Path
import subprocess
assert not Path("/app/.env").exists()
assert not Path("/app/app/data/products.json").exists()
try:
    app.loader.load_measurer()
    raise AssertionError("An image without mounted models must fail clearly")
except RuntimeError as error:
    assert "모델·데이터 파일이 없습니다" in str(error), error
subprocess.run(["python", "-m", "pip", "check"], check=True)
'
docker network create "$name" >/dev/null
docker run --detach --name "$name-kafka" --network "$name" --network-alias kafka \
  --env CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk --env KAFKA_NODE_ID=1 \
  --env KAFKA_PROCESS_ROLES=broker,controller \
  --env KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
  --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka:4.3.1 >/dev/null
deadline=$((SECONDS + 120))
until docker exec "$name-kafka" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do
  [ "$SECONDS" -lt "$deadline" ] || { echo 'Test Kafka did not start.' >&2; exit 1; }
  sleep 2
done
for topic in ci.requests ci.results; do
  docker exec "$name-kafka" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null
done
# Proprietary training artifacts are absent in CI. Replace only the measurer;
# the actual worker poll, result publication, offset commit and healthcheck run unchanged.
docker run --detach --name "$name-worker" --network "$name" \
  --env KAFKA_BOOTSTRAP_SERVERS=kafka:9092 --env DL_REQUEST_TOPIC=ci.requests \
  --env DL_RESULT_TOPIC=ci.results --env DL_WORKER_GROUP=ci-worker \
  --entrypoint python "$image" -c '
import app.loader
app.loader.load_measurer = lambda: ("cpu", lambda images: {"status": "known", "barcode": "ci-item", "size": {"type": "measured", "w_cm": 3.0, "d_cm": 2.0, "h_cm": 1.0, "volume_cm3": 6.0}})
from app.worker import main
main()
' >/dev/null
deadline=$((SECONDS + 90))
until [ "$(docker inspect --format '{{.State.Health.Status}}' "$name-worker")" = healthy ]; do
  [ "$(docker inspect --format '{{.State.Running}}' "$name-worker")" = true ] || { echo 'Test worker exited.' >&2; exit 1; }
  [ "$SECONDS" -lt "$deadline" ] || { echo 'Test worker did not become ready.' >&2; exit 1; }
  sleep 2
done

docker exec -i "$name-worker" python - <<'PY'
import json, time
from PIL import Image
from confluent_kafka import Consumer, Producer
refs = []
for camera in (1, 2, 3):
    path = f"/tmp/ci-cam{camera}.jpg"
    Image.new("RGB", (8, 8)).save(path)
    refs.append(path)
producer = Producer({"bootstrap.servers": "kafka:9092"})
consumer = Consumer({"bootstrap.servers": "kafka:9092", "group.id": "ci-result-check", "auto.offset.reset": "earliest",
                     "enable.auto.commit": False, "isolation.level": "read_committed"})
consumer.subscribe(["ci.results"])
for event, images in [("ci-measured", refs), ("ci-unreadable", ["/does/not/exist"] * 3)]:
    producer.produce("ci.requests", key=event, value=json.dumps({"schemaVersion": 1, "eventId": event, "warehouseId": "ci-warehouse", "imageRefs": images}))
assert producer.flush(15) == 0
results = {}
deadline = time.monotonic() + 45
try:
    while len(results) < 2 and time.monotonic() < deadline:
        message = consumer.poll(1)
        if message is not None and not message.error():
            result = json.loads(message.value())
            results[result["eventId"]] = result
finally:
    consumer.close()
assert results["ci-measured"]["outcome"] == "MEASURED", results
assert results["ci-measured"]["size"]["volumeCm3"] == 6.0, results
assert results["ci-unreadable"]["failureReason"] == "IMAGE_UNREADABLE", results
print("Kafka request/result round trips, image errors and worker readiness checks passed.")
PY
