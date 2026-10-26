# 차곡 Vision Data

추론 서버와 Kafka Worker가 시작할 때 읽는 모델·데이터 파일 안내입니다.

## Features

- **추론 데이터 준비** — [item-dimension-training](https://github.com/chagokAI/item-dimension-training)의 `outputs/`에서 생성한 파일을 이 폴더로 복사합니다.
- **시작 시 검증** — 필수 파일 7개가 없으면 누락 목록과 함께 오류를 반환합니다. `holdout.json`은 평가용 선택 파일입니다.
- **예시 제공** — `*.example.json`은 형식 확인용 가상 데이터입니다. 실제 파일은 AI Hub 데이터셋 파생물이므로 Git에 올리지 않습니다.

## Structure

| 파일                                          | 역할·형식                                                                                                     |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `background_cam1.png` ~ `background_cam3.png` | 카메라별 빈 바닥 배경, RGB PNG 960×540                                                                        |
| `roi.json`                                    | 원본 1920×1080 좌표의 물체 영역. `{"1": [x0, y0, x1, y1], "2": [...], "3": [...]}` ([예시](roi.example.json)) |
| `product_db.npz`                              | L2 정규화한 `embeddings`: float32 (N, 384), `product_index`: int (N,), `products.json` 인덱스                 |
| `products.json`                               | 상품의 `kan_code`, `name`, `barcode`, 내림차순 `dims_cm: [w, d, h]` ([예시](products.example.json))           |
| `volume_model.joblib`                         | `MultiOutputRegressor(HistGradientBoostingRegressor)`, 기하 특징 21개 → log(치수 3개)                         |
| `holdout.json`                                | 평가용 미등록 상품 ID 목록 `["<KAN>_<바코드>", ...]`. 서버는 읽지 않음 ([예시](holdout.example.json))         |

[파일 준비·실행 안내](../../docs/guide.md)
