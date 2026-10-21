"""입고 사진 분석 요청을 Kafka에서 받아 측정하고, 결과를 결과 토픽으로 돌려주는 Worker.

Spring이 요청 토픽에 이미지 경로 3개를 실어 보내면, Worker가 경로에서 사진을 직접 읽어 측정한다.
결과(식별 바코드·치수 또는 실패 사유)는 결과 토픽으로 보내고, 박스 선택과 DB 반영은 Spring이 한다.
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
- DEVICE, KNOWN_THRESHOLD  HTTP 서버와 같다(app/loader.py)
"""
import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

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


class PublishError(Exception):
    pass


def publish(producer, topic: str, key: str, message: dict, timeout: float = 30.0) -> None:
    """결과를 보내고 브로커 확인까지 기다린다. 확인을 못 받으면 요청 오프셋을 커밋하지 않도록 예외로 알린다."""
    errors = []
    producer.produce(topic, key=key.encode("utf-8"),
                     value=json.dumps(message, ensure_ascii=False).encode("utf-8"),
                     on_delivery=lambda err, _msg: errors.append(err) if err else None)
    remaining = producer.flush(timeout)
    if remaining or errors:
        raise PublishError(f"결과 발행 실패: eventId={key}, 미전송={remaining}, 오류={errors}")


def run(settings: Settings, measure: Callable[[list[Image.Image]], dict]) -> None:
    from confluent_kafka import Consumer, Producer

    # 결과 발행 확인 → 요청 커밋 순서로 처리한다. 중간에 죽으면 커밋되지 않은 요청을 다시 처리하므로(최소 1회),
    # 같은 결과가 두 번 갈 수 있다. Spring이 PENDING인 작업만 반영해 중복을 걸러 낸다
    # 로컬 compose는 Kafka 포트를 IPv4(127.0.0.1)에만 연다. localhost가 IPv6(::1)로 먼저 풀려 연결 실패 로그가
    # 쌓이므로 기본값을 IPv4로 둔다
    common = {"bootstrap.servers": settings.bootstrap_servers,
              "broker.address.family": settings.broker_address_family}
    consumer = Consumer({
        **common,
        "group.id": settings.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": settings.auto_offset_reset,
    })
    producer = Producer({
        **common,
        "acks": "all",
        "enable.idempotence": True,
        # 기본값(5분) 대신 30초 안에 실패를 드러낸다. 그동안 요청 처리가 멈추기 때문이다
        "delivery.timeout.ms": 30000,
    })
    stopping = False

    def stop(signum, _frame):
        nonlocal stopping
        log.info("종료 신호(%s) 수신, 처리 중인 요청을 마치고 멈춘다", signum)
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    consumer.subscribe([settings.request_topic])
    log.info("Worker 시작: request=%s, result=%s, group=%s", settings.request_topic, settings.result_topic,
             settings.group_id)
    try:
        while not stopping:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("Kafka 수신 오류: %s", msg.error())
                continue
            out = handle(msg.value(), measure, settings)
            if out is not None:
                key, message = out
                publish(producer, settings.result_topic, key, message)
                log.info("결과 발행: eventId=%s, outcome=%s, reason=%s, latencyMs=%s", key, message["outcome"],
                         message["failureReason"], message["latencyMs"])
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        producer.flush(10)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from app.loader import load_measurer

    device, measurer = load_measurer()
    log.info("모델 준비 완료: device=%s", device)
    run(Settings.from_env(), measurer)


if __name__ == "__main__":
    main()
