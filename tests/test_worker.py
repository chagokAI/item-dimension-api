"""Worker의 요청 1건 처리(handle)를 모델·Kafka 없이 확인한다.

측정기 자리에 응답을 마음대로 정할 수 있는 가짜 함수를 넣는다. 실행: .venv/bin/python -m unittest
"""
import json
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from app.worker import Settings, handle

SETTINGS = Settings(max_attempts=3, retry_delay_seconds=0)
KNOWN = {"status": "known", "barcode": "8809251219122", "product_name": "테스트 상품",
         "size": {"type": "measured", "w_cm": 21.4, "d_cm": 8.5, "h_cm": 4.3, "volume_cm3": 782.2}}
UNKNOWN = {"status": "unknown", "barcode": None, "product_name": None,
           "size": {"type": "estimated", "w_cm": 26.2, "d_cm": 13.2, "h_cm": 2.9, "volume_cm3": 997.1}}


class HandleTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.refs = []
        for cam in (1, 2, 3):
            path = Path(self.dir.name) / f"cam{cam}.jpg"
            Image.new("RGB", (8, 8)).save(path)
            self.refs.append(str(path))

    def tearDown(self):
        self.dir.cleanup()

    def request(self, **overrides):
        body = {"schemaVersion": 1, "eventId": "evt-1", "warehouseId": "wh-1", "imageRefs": self.refs}
        body.update(overrides)
        return json.dumps(body).encode()

    def test_known_item_is_returned_as_measured_with_camel_case_size(self):
        received = []

        key, result = handle(self.request(), lambda images: received.append(images) or KNOWN, SETTINGS)

        self.assertEqual(key, "evt-1")
        self.assertEqual(len(received[0]), 3)
        self.assertEqual(result["schemaVersion"], 1)
        self.assertEqual(result["warehouseId"], "wh-1")
        self.assertEqual(result["outcome"], "MEASURED")
        self.assertEqual(result["dlStatus"], "known")
        self.assertEqual(result["barcode"], "8809251219122")
        self.assertEqual(result["size"], {"type": "measured", "widthCm": 21.4, "depthCm": 8.5, "heightCm": 4.3,
                                          "volumeCm3": 782.2})
        self.assertIsNone(result["failureReason"])
        self.assertTrue(result["measuredAt"].endswith("Z"))
        self.assertIsInstance(result["latencyMs"], int)

    def test_unknown_item_is_still_measured(self):
        _, result = handle(self.request(), lambda images: UNKNOWN, SETTINGS)

        self.assertEqual(result["outcome"], "MEASURED")
        self.assertEqual(result["dlStatus"], "unknown")
        self.assertIsNone(result["barcode"])
        self.assertEqual(result["size"]["type"], "estimated")

    def test_missing_image_fails_without_calling_the_model(self):
        refs = [self.refs[0], str(Path(self.dir.name) / "missing.jpg"), self.refs[2]]
        calls = []

        _, result = handle(self.request(imageRefs=refs), lambda images: calls.append(1) or KNOWN, SETTINGS)

        self.assertEqual(result["outcome"], "FAILED")
        self.assertEqual(result["failureReason"], "IMAGE_UNREADABLE")
        self.assertIn("cam2", result["failureMessage"])
        self.assertIsNone(result["size"])
        self.assertEqual(calls, [])

    def test_file_that_is_not_an_image_is_unreadable(self):
        Path(self.refs[0]).write_bytes(b"not an image")

        _, result = handle(self.request(), lambda images: KNOWN, SETTINGS)

        self.assertEqual(result["failureReason"], "IMAGE_UNREADABLE")

    def test_model_rejection_is_not_retried(self):
        calls = []

        def reject(images):
            calls.append(1)
            raise ValueError("cam1 이미지 해상도가 8x8입니다.")

        _, result = handle(self.request(), reject, SETTINGS)

        self.assertEqual(result["failureReason"], "DL_REJECTED")
        self.assertEqual(len(calls), 1)

    def test_unexpected_error_is_retried_then_succeeds(self):
        calls = []

        def flaky(images):
            calls.append(1)
            if len(calls) < 3:
                raise RuntimeError("MPS out of memory")
            return KNOWN

        _, result = handle(self.request(), flaky, SETTINGS, sleep=lambda _: None)

        self.assertEqual(result["outcome"], "MEASURED")
        self.assertEqual(len(calls), 3)

    def test_unexpected_error_fails_after_max_attempts(self):
        calls = []

        def broken(images):
            calls.append(1)
            raise RuntimeError("device lost")

        _, result = handle(self.request(), broken, SETTINGS, sleep=lambda _: None)

        self.assertEqual(result["failureReason"], "DL_UNAVAILABLE")
        self.assertEqual(len(calls), 3)

    def test_unexpected_error_outside_the_model_call_becomes_a_failed_result(self):
        # 측정기가 계약과 다른 응답을 돌려줘도 Worker가 죽지 않고 FAILED 결과를 보내야 한다
        _, result = handle(self.request(), lambda images: {"status": "known"}, SETTINGS)

        self.assertEqual(result["outcome"], "FAILED")
        self.assertEqual(result["failureReason"], "DL_UNAVAILABLE")

    def test_decompression_bomb_is_unreadable(self):
        original = Image.MAX_IMAGE_PIXELS
        Image.MAX_IMAGE_PIXELS = 10
        try:
            _, result = handle(self.request(), lambda images: KNOWN, SETTINGS)
        finally:
            Image.MAX_IMAGE_PIXELS = original

        self.assertEqual(result["failureReason"], "IMAGE_UNREADABLE")

    def test_max_attempts_below_one_is_rejected_at_startup(self):
        with self.assertRaises(ValueError):
            Settings(max_attempts=0)

    def test_request_outside_contract_fails_as_invalid_request(self):
        for overrides in ({"schemaVersion": 2}, {"imageRefs": self.refs[:2]}, {"imageRefs": None}):
            with self.subTest(overrides=overrides):
                key, result = handle(self.request(**overrides), lambda images: KNOWN, SETTINGS)

                self.assertEqual(key, "evt-1")
                self.assertEqual(result["failureReason"], "INVALID_REQUEST")
                self.assertEqual(result["warehouseId"], "wh-1")

    def test_request_without_event_id_is_dropped(self):
        for raw in (b"{not json", json.dumps({"schemaVersion": 1}).encode(), None, b"[]"):
            with self.subTest(raw=raw):
                self.assertIsNone(handle(raw, lambda images: KNOWN, SETTINGS))


if __name__ == "__main__":
    unittest.main()
