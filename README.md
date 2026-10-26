# Chagok Vision

Multi-camera vision for item recognition and dimension estimation.

차곡 비전은 고정 카메라 3대의 입고 이미지를 분석해 상품과 치수·부피를 반환하는 추론 서비스입니다.
DINOv2로 등록 상품을 식별하고, 처음 보는 물품은 기하 특징과 회귀 모델로 치수를 추정합니다. 차곡 서비스에는 Kafka Worker로 연결됩니다.

[실행 가이드](docs/guide.md) · [모델 준비](app/data/README.md) · [배포 안내](docs/cicd.md)

## ✨ 주요 기능

- **사진으로 찾는 등록 상품** — 이미지 임베딩을 비교해 바코드·상품명과 등록된 실측 치수를 반환합니다.
- **처음 보는 물품도 치수 추정** — 배경에서 물체 영역을 분리하고 기하 특징으로 치수·부피를 추정합니다.
- **업무 흐름에 맞춘 비동기 분석** — Kafka 요청을 소비하고 분석 결과를 발행하며 재시도와 트랜잭션을 처리합니다.
- **직접 확인하는 추론 결과** — FastAPI의 이미지 측정 API와 상태 확인 API로 추론을 점검합니다.

## 🗂️ 프로젝트 구성

```text
item-dimension-api/
├── app/
│   ├── worker.py     # Kafka 요청 소비와 결과 발행
│   ├── main.py       # 이미지 측정·상태 확인 API
│   ├── measure.py    # 상품 식별과 치수 추론
│   ├── segment.py    # 배경 차분과 물체 영역 분리
│   ├── geometry.py   # 이미지 크롭과 기하 특징 추출
│   ├── loader.py     # 모델·데이터 검증과 로딩
│   └── data/         # 모델·상품 데이터와 형식 예시
├── tests/            # Worker 처리와 트랜잭션 검증
├── scripts/          # API 호출 예제와 배포 스크립트
└── docs/             # 추론·실행·배포 안내
```

모델과 데이터는 [학습 저장소](https://github.com/chagokAI/item-dimension-training)에서 생성해 별도로 준비합니다.
