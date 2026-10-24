"""입고 사진 분석 요청을 Kafka에서 받아 측정하고, 결과를 결과 토픽으로 돌려주는 Worker.

Spring이 요청 토픽에 이미지 경로 3개를 실어 보내면, Worker가 경로에서 사진을 직접 읽어 측정한다.
결과(식별 바코드·치수 또는 실패 사유)는 결과 토픽으로 보내고, 박스 선택과 DB 반영은 Spring이 한다.
결과 발행과 요청 오프셋 커밋은 한 Kafka 트랜잭션으로 확정한다. Worker가 중간에 죽어도 결과는 정확히 한 번만 보인다.
메시지 형식은 chagokAI/server의 chagok/docs/dl-kafka-contract.md를 따른다.

실행: .venv/bin/python -m app.worker   (Spring을 먼저 띄워 토픽이 만들어진 뒤)

설정(환경변수)
- KAFKA_BOOTSTRAP_SERVERS  기본 localhost:9092
- DL_REQUEST_TOPIC         기본 chagok.dl.analysis-request.v1
- DL_RESULT_TOPIC          기본 chagok.dl.analysis-result.v1
- DL_WORKER_GROUP          기본 chagok-dl-worker. Worker를 여러 개 띄울 때도 같은 값을 써야 요청이 나뉘어 처리된다
- DL_MAX_ATTEMPTS          기본 3. 예상하지 못한 추론 오류의 최대 시도 횟수
- DL_RETRY_DELAY_SECONDS   기본 2
- DL_AUTO_OFFSET_RESET     기본 earliest. 그룹의 커밋 위치가 없을 때 시작점. 아래 README의 「처음 띄울 때」 참고
- DL_BROKER_ADDRESS_FAMILY 기본 v4. 로컬 compose가 IPv4에만 포트를 열기 때문이다. IPv6 브로커면 any
- DL_GROUP_INSTANCE_ID     Worker마다 다른 고정 이름. 주면 정적 구성원으로 동작해, 같은 이름으로 다시 띄우면 재배정 없이
                           바로 이어받는다. 두 Worker에 같은 이름을 주면 한쪽이 차단된다
- DL_SESSION_TIMEOUT_MS    기본 10000. 죽은 Worker를 그룹에서 빼기까지 기다리는 시간(라이브러리 기본 45초)
- DEVICE, KNOWN_THRESHOLD  HTTP 서버와 같다(app/loader.py)
"""
import json
import logging
import os
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from confluent_kafka import KafkaException, TopicPartition
from PIL import Image

SCHEMA_VERSION = 1
CAMERA_COUNT = 3

log = logging.getLogger("app.worker")


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = "localhost:9092"
    request_topic: str = "chagok.dl.analysis-request.v1"
    result_topic: str = "chagok.dl.analysis-result.v1"
    group_id: str = "chagok-dl-worker"
    max_attempts: int = 3
    retry_delay_seconds: float = 2.0
    auto_offset_reset: str = "earliest"
    broker_address_family: str = "v4"
    group_instance_id: str | None = None
    session_timeout_ms: int = 10000
    transaction_timeout_ms: int = 30000

    def __post_init__(self):
        # 0 이하이면 추론을 한 번도 시도하지 않아 모든 요청이 처리되지 못한다. 기동 시점에 막는다
        if self.max_attempts < 1:
            raise ValueError(f"DL_MAX_ATTEMPTS는 1 이상이어야 한다: {self.max_attempts}")

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            request_topic=os.getenv("DL_REQUEST_TOPIC", cls.request_topic),
            result_topic=os.getenv("DL_RESULT_TOPIC", cls.result_topic),
            group_id=os.getenv("DL_WORKER_GROUP", cls.group_id),
            max_attempts=int(os.getenv("DL_MAX_ATTEMPTS", cls.max_attempts)),
            retry_delay_seconds=float(os.getenv("DL_RETRY_DELAY_SECONDS", cls.retry_delay_seconds)),
            auto_offset_reset=os.getenv("DL_AUTO_OFFSET_RESET", cls.auto_offset_reset),
            broker_address_family=os.getenv("DL_BROKER_ADDRESS_FAMILY", cls.broker_address_family),
            group_instance_id=os.getenv("DL_GROUP_INSTANCE_ID") or None,
            session_timeout_ms=int(os.getenv("DL_SESSION_TIMEOUT_MS", cls.session_timeout_ms)),
        )


@dataclass(frozen=True)
class Request:
    event_id: str
    warehouse_id: str | None
    image_refs: list[str]


class Failure(Exception):
    """측정 결과 대신 FAILED 결과로 돌려줄 실패. reason은 계약의 failureReason 코드다."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


class InvalidRequest(Failure):
    def __init__(self, message: str, event_id: str | None = None, warehouse_id: str | None = None):
        super().__init__("INVALID_REQUEST", message)
        self.event_id = event_id
        self.warehouse_id = warehouse_id


def parse_request(raw: bytes | None) -> Request:
    try:
        body = json.loads(raw or b"")
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise InvalidRequest(f"요청이 JSON이 아니다: {e}")
    if not isinstance(body, dict):
        raise InvalidRequest("요청이 JSON 객체가 아니다")
    event_id, warehouse_id = body.get("eventId"), body.get("warehouseId")
    if not isinstance(event_id, str) or not event_id.strip():
        raise InvalidRequest("eventId가 없다")
    if body.get("schemaVersion") != SCHEMA_VERSION:
        raise InvalidRequest(f"지원하지 않는 schemaVersion: {body.get('schemaVersion')}", event_id, warehouse_id)
    refs = body.get("imageRefs")
    if not isinstance(refs, list) or len(refs) != CAMERA_COUNT or not all(isinstance(r, str) and r for r in refs):
        raise InvalidRequest(f"imageRefs는 cam1~3 순서의 경로 {CAMERA_COUNT}개여야 한다", event_id, warehouse_id)
    return Request(event_id, warehouse_id, refs)


def read_images(refs: list[str]) -> list[Image.Image]:
    images = []
    for cam, ref in enumerate(refs, start=1):
        try:
            with Image.open(ref) as image:
                images.append(image.convert("RGB"))
        # 파일 없음·권한·이미지 아님(UnidentifiedImageError)은 OSError다. 지나치게 큰 이미지는 OSError가 아닌
        # DecompressionBombError라 따로 잡는다
        except (OSError, Image.DecompressionBombError) as e:
            raise Failure("IMAGE_UNREADABLE", f"cam{cam} 이미지를 읽을 수 없다: {ref} ({e})")
    return images


def measure_with_retry(measure: Callable[[list[Image.Image]], dict], images: list[Image.Image],
                       settings: Settings, sleep: Callable[[float], None]) -> dict:
    # Measurer는 입력이 잘못되면 ValueError를 낸다(해상도·물체 미검출). 다시 해도 같으므로 바로 거부로 돌려준다.
    # 그 밖의 예외는 장치·메모리 같은 일시 문제일 수 있어 정해진 횟수만큼 다시 시도한다
    for attempt in range(1, settings.max_attempts + 1):
        try:
            return measure(images)
        except ValueError as e:
            raise Failure("DL_REJECTED", str(e))
        except Exception as e:
            if attempt == settings.max_attempts:
                raise Failure("DL_UNAVAILABLE", f"추론 {attempt}회 실패: {e!r}")
            log.warning("추론 실패, %s초 뒤 다시 시도 (%d/%d): %r", settings.retry_delay_seconds, attempt,
                        settings.max_attempts, e)
            sleep(settings.retry_delay_seconds)
    raise AssertionError("도달하지 않는다")


def result_message(event_id: str, warehouse_id: str | None, latency_ms: int,
                   response: dict | None = None, failure: Failure | None = None) -> dict:
    size = response["size"] if response else None
    return {
        "schemaVersion": SCHEMA_VERSION,
        "eventId": event_id,
        "warehouseId": warehouse_id,
        "outcome": "MEASURED" if response else "FAILED",
        "dlStatus": response["status"] if response else None,
        "barcode": response["barcode"] if response else None,
        "size": {
            "type": size["type"],
            "widthCm": size["w_cm"],
            "depthCm": size["d_cm"],
            "heightCm": size["h_cm"],
            "volumeCm3": size["volume_cm3"],
        } if size else None,
        "failureReason": failure.reason if failure else None,
        "failureMessage": str(failure) if failure else None,
        "latencyMs": latency_ms,
        "measuredAt": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }


def handle(raw: bytes | None, measure: Callable[[list[Image.Image]], dict], settings: Settings,
           sleep: Callable[[float], None] = time.sleep) -> tuple[str, dict] | None:
    """요청 메시지 1건을 처리해 (결과 키, 결과 메시지)를 돌려준다. 결과를 보낼 eventId가 없으면 None."""
    started = time.perf_counter()
    try:
        request = parse_request(raw)
    except InvalidRequest as e:
        if e.event_id is None:
            log.error("요청 버림(eventId 없음): %s", e)
            return None
        return e.event_id, result_message(e.event_id, e.warehouse_id, 0, failure=e)
    # 여기서 예외가 새어 나가면 Worker가 커밋하지 못한 채 죽고, 다시 띄워도 같은 요청에서 또 죽는다.
    # 그래서 예상하지 못한 예외도 FAILED 결과로 바꿔 돌려준다
    def latency_ms() -> int:
        return round((time.perf_counter() - started) * 1000)

    try:
        response = measure_with_retry(measure, read_images(request.image_refs), settings, sleep)
        message = result_message(request.event_id, request.warehouse_id, latency_ms(), response=response)
    except Failure as e:
        message = result_message(request.event_id, request.warehouse_id, latency_ms(), failure=e)
    except Exception as e:
        log.exception("요청 처리 중 예상하지 못한 오류: eventId=%s", request.event_id)
        failure = Failure("DL_UNAVAILABLE", f"예상하지 못한 오류: {e!r}")
        message = result_message(request.event_id, request.warehouse_id, latency_ms(), failure=failure)
    return request.event_id, message


def consumer_config(settings: Settings) -> dict:
    """요청 소비 설정. 오프셋은 결과 트랜잭션이 함께 커밋하므로 자동 커밋을 끈다."""
    config = {
        "bootstrap.servers": settings.bootstrap_servers,
        # 로컬 compose는 Kafka 포트를 IPv4(127.0.0.1)에만 연다. localhost가 IPv6(::1)로 먼저 풀려 연결 실패 로그가
        # 쌓이므로 기본값을 IPv4로 둔다
        "broker.address.family": settings.broker_address_family,
        "group.id": settings.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": settings.auto_offset_reset,
        # 확정된 트랜잭션의 메시지만 읽는다. 요청은 트랜잭션 없이 발행되므로 그대로 보인다
        "isolation.level": "read_committed",
        # 기본 45초면 강제 종료된 Worker의 파티션을 그만큼 아무도 처리하지 못한다(실측). 10초로 줄인다
        "session.timeout.ms": settings.session_timeout_ms,
    }
    if settings.group_instance_id:
        config["group.instance.id"] = settings.group_instance_id
    return config


def producer_config(settings: Settings) -> dict:
    """결과 발행 설정. transactional.id가 고정이면 같은 이름으로 다시 뜬 Worker가 죽은 Worker의 미완료 트랜잭션을 끊는다."""
    instance = settings.group_instance_id or uuid.uuid4().hex
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "broker.address.family": settings.broker_address_family,
        "transactional.id": f"{settings.group_id}-{instance}",
        "enable.idempotence": True,
        "acks": "all",
        # 트랜잭션은 발행과 오프셋만 감싼다. 열려 있는 동안 read_committed 소비자가 그 파티션의 뒤 결과를 못 읽으므로
        # 짧게 끝내고, Worker가 멈춘 경우에도 30초 안에 브로커가 중단시킨다
        "transaction.timeout.ms": settings.transaction_timeout_ms,
    }


def process(msg, consumer, producer, measure: Callable[[list[Image.Image]], dict], settings: Settings,
            sleep: Callable[[float], None] = time.sleep) -> None:
    """요청 1건을 처리하고 결과 발행과 다음 오프셋 커밋을 한 트랜잭션으로 확정한다.

    추론은 트랜잭션 밖에서 끝낸다. 트랜잭션이 중단되면 결과는 소비자에게 보이지 않고 오프셋도 그대로라,
    같은 요청 위치로 되돌려 다시 처리한다. 결과가 두 번 확정되는 일은 생기지 않는다.
    """
    out = handle(msg.value(), measure, settings, sleep)
    producer.begin_transaction()
    try:
        if out is not None:
            key, message = out
            producer.produce(settings.result_topic, key=key.encode("utf-8"),
                             value=json.dumps(message, ensure_ascii=False).encode("utf-8"))
        producer.send_offsets_to_transaction(
            [TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)], consumer.consumer_group_metadata())
        _commit(producer)
    except KafkaException as e:
        error = e.args[0]
        # 일시 오류(코디네이터 이동 등)는 중단 요구 표시 없이 올 수 있다. 중단하고 다시 처리하면 되므로 Worker를 멈추지 않는다
        if error.fatal() or not (error.txn_requires_abort() or error.retriable()):
            raise
        log.warning("결과 트랜잭션 중단, 같은 요청을 다시 처리한다: offset=%s, cause=%s", msg.offset(), error)
        producer.abort_transaction()
        try:
            consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
        except KafkaException as seek_error:
            # 재배정으로 이 파티션을 잃었다. 새 담당 Worker가 커밋된 위치, 즉 이 요청부터 다시 처리한다
            log.warning("파티션을 잃어 되감지 않는다: partition=%s, cause=%s", msg.partition(), seek_error.args[0])
        return
    if out is not None:
        log.info("결과 확정: eventId=%s, outcome=%s, reason=%s, latencyMs=%s", out[0], out[1]["outcome"],
                 out[1]["failureReason"], out[1]["latencyMs"])


def _commit(producer) -> None:
    # 일시 오류(시간 초과 등)는 같은 트랜잭션의 커밋을 다시 시도하면 된다. 중단이 필요한 오류는 호출자가 처리한다
    while True:
        try:
            producer.commit_transaction()
            return
        except KafkaException as e:
            if not e.args[0].retriable():
                raise
            log.warning("결과 트랜잭션 커밋 재시도: %s", e.args[0])


def run(settings: Settings, measure: Callable[[list[Image.Image]], dict]) -> None:
    from confluent_kafka import Consumer, Producer

    consumer = Consumer(consumer_config(settings))
    producer = Producer(producer_config(settings))
    # 같은 transactional.id의 이전 트랜잭션이 열려 있으면 여기서 끊는다(좀비 차단)
    producer.init_transactions()
    stopping = False

    def stop(signum, _frame):
        nonlocal stopping
        log.info("종료 신호(%s) 수신, 처리 중인 요청을 마치고 멈춘다", signum)
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    consumer.subscribe([settings.request_topic])
    log.info("Worker 시작: request=%s, result=%s, group=%s, instance=%s", settings.request_topic, settings.result_topic,
             settings.group_id, settings.group_instance_id or "동적")
    try:
        while not stopping:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("Kafka 수신 오류: %s", msg.error())
                continue
            process(msg, consumer, producer, measure, settings)
    finally:
        consumer.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from app.loader import load_measurer

    device, measurer = load_measurer()
    log.info("모델 준비 완료: device=%s", device)
    run(Settings.from_env(), measurer)


if __name__ == "__main__":
    main()
