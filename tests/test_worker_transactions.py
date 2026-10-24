"""Worker가 결과 발행과 요청 오프셋 커밋을 한 Kafka 트랜잭션으로 묶는지 브로커 없이 확인한다.

예전에는 결과 발행 확인 뒤 오프셋을 따로 커밋했다. 그 사이에 Worker가 죽으면 같은 요청을 다시 처리해 같은 결과가
두 번 나갔고, 그 중복은 Spring이 DB 상태로 걸러 냈다. Kafka 안에서 생긴 중복은 Kafka 트랜잭션으로 없앤다.
실행: .venv/bin/python -m pytest -q
"""
import json
import unittest

from confluent_kafka import KafkaError, KafkaException, TopicPartition

from app.worker import Settings, consumer_config, process, producer_config

SETTINGS = Settings(max_attempts=1, retry_delay_seconds=0)
KNOWN = {"status": "known", "barcode": "8809251219122", "product_name": "테스트 상품",
         "size": {"type": "measured", "w_cm": 21.4, "d_cm": 8.5, "h_cm": 4.3, "volume_cm3": 782.2}}


class FakeMessage:
    def __init__(self, value, offset=41, partition=2, topic="chagok.dl.analysis-request.v1"):
        self._value, self._offset, self._partition, self._topic = value, offset, partition, topic

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def topic(self):
        return self._topic


class FakeConsumer:
    def __init__(self, revoked=False):
        self.seeks = []
        self.revoked = revoked

    def consumer_group_metadata(self):
        return "group-metadata"

    def seek(self, partition):
        if self.revoked:
            raise KafkaException(KafkaError(KafkaError._STATE, "partition not assigned"))
        self.seeks.append((partition.topic, partition.partition, partition.offset))


class FakeProducer:
    def __init__(self, fail_commit=None, fail_offsets=None):
        self.calls = []
        self.fail_commit = list(fail_commit or [])
        self.fail_offsets = list(fail_offsets or [])

    def begin_transaction(self):
        self.calls.append(("begin",))

    def produce(self, topic, key, value):
        self.calls.append(("produce", topic, key.decode(), json.loads(value)))

    def send_offsets_to_transaction(self, offsets, metadata, timeout=None):
        if self.fail_offsets:
            raise KafkaException(self.fail_offsets.pop(0))
        self.calls.append(("offsets", [(p.topic, p.partition, p.offset) for p in offsets], metadata))

    def commit_transaction(self, timeout=None):
        if self.fail_commit:
            raise KafkaException(self.fail_commit.pop(0))
        self.calls.append(("commit",))

    def abort_transaction(self, timeout=None):
        self.calls.append(("abort",))


def request(event_id="evt-1"):
    return json.dumps({"schemaVersion": 1, "eventId": event_id, "warehouseId": "wh-1",
                       "imageRefs": ["/missing/1.jpg", "/missing/2.jpg", "/missing/3.jpg"]}).encode()


class TransactionTest(unittest.TestCase):

    def test_result_and_next_request_offset_commit_together(self):
        consumer, producer = FakeConsumer(), FakeProducer()

        process(FakeMessage(request()), consumer, producer, lambda images: KNOWN, SETTINGS)

        self.assertEqual([call[0] for call in producer.calls], ["begin", "produce", "offsets", "commit"])
        _, topic, key, result = producer.calls[1]
        self.assertEqual((topic, key), (SETTINGS.result_topic, "evt-1"))
        # 이미지가 없어 측정은 실패하지만, 실패도 결과 메시지로 같은 트랜잭션에 실린다
        self.assertEqual(result["failureReason"], "IMAGE_UNREADABLE")
        self.assertEqual(producer.calls[2][1], [("chagok.dl.analysis-request.v1", 2, 42)])
        self.assertEqual(producer.calls[2][2], "group-metadata")
        self.assertEqual(consumer.seeks, [])

    def test_request_without_event_id_only_moves_the_offset(self):
        consumer, producer = FakeConsumer(), FakeProducer()

        process(FakeMessage(b"not json"), consumer, producer, lambda images: KNOWN, SETTINGS)

        self.assertEqual([call[0] for call in producer.calls], ["begin", "offsets", "commit"])

    def test_retriable_commit_failure_is_retried_in_the_same_transaction(self):
        consumer = FakeConsumer()
        producer = FakeProducer(fail_commit=[KafkaError(KafkaError._TIMED_OUT, "slow", retriable=True)])

        process(FakeMessage(request()), consumer, producer, lambda images: KNOWN, SETTINGS)

        self.assertEqual([call[0] for call in producer.calls], ["begin", "produce", "offsets", "commit"])

    def test_abortable_failure_aborts_and_rewinds_to_the_same_request(self):
        consumer = FakeConsumer()
        producer = FakeProducer(fail_commit=[KafkaError(KafkaError._FENCED, "abortable", txn_requires_abort=True)])

        process(FakeMessage(request()), consumer, producer, lambda images: KNOWN, SETTINGS)

        self.assertEqual([call[0] for call in producer.calls], ["begin", "produce", "offsets", "abort"])
        # 결과는 보이지 않게 버려지고 오프셋도 그대로라 같은 요청을 다시 처리한다
        self.assertEqual(consumer.seeks, [("chagok.dl.analysis-request.v1", 2, 41)])

    def test_retriable_offset_failure_aborts_and_rewinds_instead_of_stopping(self):
        # 코디네이터 이동 같은 일시 오류는 중단 요구 표시 없이 올 수 있다. Worker를 죽이지 않고 같은 요청을 다시 처리한다
        consumer = FakeConsumer()
        producer = FakeProducer(fail_offsets=[KafkaError(KafkaError._TIMED_OUT, "coordinator moving", retriable=True)])

        process(FakeMessage(request()), consumer, producer, lambda images: KNOWN, SETTINGS)

        self.assertEqual([call[0] for call in producer.calls], ["begin", "produce", "abort"])
        self.assertEqual(consumer.seeks, [("chagok.dl.analysis-request.v1", 2, 41)])

    def test_abort_after_rebalance_skips_the_rewind_of_a_revoked_partition(self):
        # 재배정으로 파티션을 잃으면 되감을 수 없다. 새 담당 Worker가 커밋된 위치부터 다시 처리하므로 그대로 넘어간다
        consumer = FakeConsumer(revoked=True)
        producer = FakeProducer(fail_commit=[KafkaError(KafkaError.ILLEGAL_GENERATION, "rebalanced", txn_requires_abort=True)])

        process(FakeMessage(request()), consumer, producer, lambda images: KNOWN, SETTINGS)

        self.assertEqual([call[0] for call in producer.calls], ["begin", "produce", "offsets", "abort"])

    def test_fatal_failure_stops_the_worker(self):
        consumer = FakeConsumer()
        producer = FakeProducer(fail_commit=[KafkaError(KafkaError._FENCED, "fenced", fatal=True)])

        with self.assertRaises(KafkaException):
            process(FakeMessage(request()), consumer, producer, lambda images: KNOWN, SETTINGS)


class ConfigTest(unittest.TestCase):

    def test_consumer_reads_committed_results_and_rejoins_quickly(self):
        config = consumer_config(Settings(group_instance_id="worker-a"))

        self.assertEqual(config["isolation.level"], "read_committed")
        self.assertFalse(config["enable.auto.commit"])
        self.assertEqual(config["session.timeout.ms"], 10000)
        self.assertEqual(config["group.instance.id"], "worker-a")

    def test_static_instance_id_gives_a_stable_transactional_id(self):
        config = producer_config(Settings(group_instance_id="worker-a"))

        self.assertEqual(config["transactional.id"], "chagok-dl-worker-worker-a")
        self.assertTrue(config["enable.idempotence"])
        self.assertEqual(config["transaction.timeout.ms"], 30000)

    def test_without_instance_id_membership_is_dynamic_and_transactional_id_is_unique(self):
        settings = Settings()

        self.assertNotIn("group.instance.id", consumer_config(settings))
        first, second = producer_config(settings)["transactional.id"], producer_config(settings)["transactional.id"]
        self.assertTrue(first.startswith("chagok-dl-worker-"))
        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
