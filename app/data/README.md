# app/data

서버가 시작할 때 읽는 모델·데이터 파일 폴더입니다.
AI Hub 데이터셋 파생물이라 **git에는 올리지 않고**, [item-dimension-training](https://github.com/chagokAI/item-dimension-training)으로 만들어서 복사합니다.

```bash
# item-dimension-training에서 3단계 실행 후 (item-dimension-api 폴더에서)
cp ../item-dimension-training/outputs/* app/data/
```

파일이 없으면 서버가 시작할 때 없는 파일 목록과 함께 에러를 냅니다.

## 필요한 파일 (8개)

| 파일 | 내용 | 형식 |
|---|---|---|
| `background_cam1.png` ~ `background_cam3.png` | cam별 빈 바닥 배경 | RGB PNG, 960x540 |
| `roi.json` | cam별 물체가 놓이는 영역 (원본 1920x1080 좌표) | `{"1": [x0, y0, x1, y1], "2": [...], "3": [...]}` → `roi.example.json` |
| `product_db.npz` | 등록 상품 사진 임베딩 | `embeddings`: float32 (N, 384), L2 정규화 / `product_index`: int (N,) → products.json의 인덱스 |
| `products.json` | 등록 상품 정보 | `[{"kan_code", "name", "barcode", "dims_cm": [w, d, h] 내림차순}]` → `products.example.json` |
| `holdout.json` | 테스트용으로 일부러 등록하지 않은 상품 ID | `["<KAN>_<바코드>", ...]` → `holdout.example.json` (서버는 읽지 않음) |
| `volume_model.joblib` | 처음 보는 물품 치수 추정 모델 | scikit-learn `MultiOutputRegressor(HistGradientBoostingRegressor)`, 입력 21개 특징 → log(치수 3개) |

`*.example.json`은 형식을 보여주기 위한 가짜 값입니다.
