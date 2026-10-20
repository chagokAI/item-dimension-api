# item-dimension-api

Inference server that identifies inbound items from 3 camera images and returns their dimensions and volume (FastAPI, DINOv2)

고정 카메라 3대(cam1~3)로 찍은 입고 물품 사진을 받아서

- **등록된 물품**이면 → KAN 코드, 상품명, **실측 치수**
- **처음 보는 물품**이면 → **추정 치수**

를 JSON으로 돌려주는 추론 서버입니다.
모델·데이터 파일(`app/data/`)은 [item-dimension-training](https://github.com/chagokAI/item-dimension-training)에서 만듭니다.

---

## 1. 처리 흐름

```
cam1~3 이미지 (1920x1080 원본)
 → ① 배경 차분으로 물체 영역 찾기                         app/segment.py
 → ② 물체 중심 정사각형 크롭 (긴 변 x 1.2)                  app/geometry.py
 → ③ DINOv2-small 임베딩 → 등록 상품 DB와 코사인 유사도      app/measure.py
 → ④ 유사도 ≥ 0.80 : 등록 물품     → DB의 실측 치수
      유사도 < 0.80 : 처음 보는 물품 → ⑤ 물체 영역 기하 특징 → 부피 모델로 치수 추정
 → JSON 응답 (치수는 w ≥ d ≥ h 내림차순)
```

---

## 2. 실행

### 2-1. 모델·데이터 파일 준비 (처음 한 번)

`app/data/`의 파일은 AI Hub 데이터셋 파생물이라 레포에 없습니다. [item-dimension-training](https://github.com/chagokAI/item-dimension-training) README대로 만든 뒤 복사합니다.

```bash
# 두 레포를 같은 폴더에 클론했다고 가정
cd item-dimension-training
#   data/에 데이터셋 배치 → scripts 3개 실행 (item-dimension-training README 참고)
cp outputs/* ../item-dimension-api/app/data/
```

파일 목록과 형식은 [app/data/README.md](app/data/README.md) 참고

### 2-2. 서버 실행

```bash
cd item-dimension-api
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000
```

- 첫 실행 시 DINOv2-small(약 85MB)을 Hugging Face에서 자동으로 받습니다.
- GPU가 있으면 자동 사용 (`DEVICE=cpu`로 강제 가능)
- 등록 물품 판정 임계값: `KNOWN_THRESHOLD` 환경변수 (기본 0.80)
- API 문서: `http://localhost:8000/docs`

---

## 3. API

### `POST /measure`

**요청** (multipart/form-data): `images` 필드에 **cam1, cam2, cam3 순서**로 이미지 3장

- 해상도는 반드시 **1920x1080 원본** (크롭·축소 금지, 다르면 400 에러)
- 용량은 JPEG 품질(80 권장)로만 줄이기

```bash
curl -X POST http://localhost:8000/measure \
  -F "images=@item_cam1.jpg" -F "images=@item_cam2.jpg" -F "images=@item_cam3.jpg"
```

**응답**

등록 물품
```json
{
  "status": "known",
  "kan_code": "01010110",
  "product_name": "오씨아니 딸기시럽 500g*12",
  "size": {"type": "measured", "w_cm": 21.4, "d_cm": 8.5, "h_cm": 4.3, "volume_cm3": 782.2}
}
```

처음 보는 물품
```json
{
  "status": "unknown",
  "kan_code": null,
  "product_name": null,
  "size": {"type": "estimated", "w_cm": 26.2, "d_cm": 13.2, "h_cm": 2.9, "volume_cm3": 997.1}
}
```

| 필드 | 등록 물품 (`known`) | 처음 보는 물품 (`unknown`) |
|---|---|---|
| `kan_code` | KAN 8자리 | `null` |
| `product_name` | 상품명 | `null` |
| `size.type` | `measured` (라벨 실측값) | `estimated` (모델 추정값) |
| `size.w_cm` ≥ `d_cm` ≥ `h_cm`, `volume_cm3` | 실측 | 추정 |

**에러 (400)**: 이미지가 3장이 아님 / 이미지를 읽을 수 없음 / 해상도가 1920x1080이 아님 / 물체를 찾지 못함

### `GET /health`

```json
{"status": "ok", "device": "cpu"}
```

### 호출 예제

```bash
.venv/bin/python scripts/try_measure.py <원천데이터 상품 폴더> [shot=2] [서버 주소]
```

데이터셋 폴더(`01.원천데이터/.../<상품ID>/<상품ID>_<shot>_<cam>.jpg`)의 한 shot(3장)을 보내고, 라벨과 비교해서 출력합니다.
예: `data/`에 Sample 데이터셋을 두고 `scripts/try_measure.py data/Sample/01.원천데이터/01_입고물품/01_가공식품/01_조미료/01010110_8809251219122`

---

## 4. 모델 / 데이터 파일 (`app/data/`, git 제외)

[item-dimension-training](https://github.com/chagokAI/item-dimension-training)의 `outputs/`를 그대로 복사해서 씁니다. 형식 예시는 `app/data/*.example.json`

| 파일 | 쓰이는 단계 | 내용 |
|---|---|---|
| `background_cam{1,2,3}.png` | ① | cam별 빈 바닥 배경 (960x540) |
| `roi.json` | ① | cam별 물체가 놓이는 바닥 영역 |
| `product_db.npz` | ③ | 등록 상품 사진 임베딩 (상품당 6장 x 384차원) |
| `products.json` | ④ | 등록 상품 정보: KAN, 상품명, 바코드, 실측 치수 |
| `holdout.json` | (테스트용) | 일부러 등록하지 않은 상품 목록 |
| `volume_model.joblib` | ⑤ | 처음 보는 물품 치수 추정 모델 |

현재 등록 상품: AI Hub Sample 입고물품 40종 중 35종 (5종은 "처음 보는 물품" 테스트용으로 미등록)

---

## 5. 성능 (Sample 40종 x 2 shot = 80건)

| 항목 | 결과 |
|---|---|
| 등록 / 처음 봄 판정 | 80/80 |
| 등록 물품 치수 | 실측값 그대로 (오차 0%) |
| 처음 보는 물품 부피 오차 | 중앙값 약 32%, ±30% 이내 약 48% |

처리 시간: CPU 기준 물품 1개(3장)당 대략 0.3~0.5초

---

## 6. 전제 조건 / 주의

- [AI Hub 물류공간 예측 데이터](https://www.aihub.or.kr/aihubdata/data/view.do?currMenu=115&topMenu=100&dataSetSn=71861)와 **같은 촬영 환경**(카메라 위치, 바닥, 조명, 1920x1080), 물품 1개씩 촬영
- `app/segment.py`, `app/geometry.py`, `app/measure.py`의 계산은 item-dimension-training의 `core/`와 **같아야** 합니다. 한쪽을 바꾸면 다른 쪽도 같이 수정하고 `app/data/`를 다시 만드세요.
- `app/data/`의 실제 파일은 AI Hub 데이터셋 파생물(상품명, 실측 치수, 배경 이미지)이라 git에 올리지 않습니다 (`.gitignore`). 배포할 서버에는 따로 복사해서 넣으세요.
