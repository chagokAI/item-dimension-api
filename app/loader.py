"""측정기(Measurer) 준비. HTTP 서버(app.main)와 Kafka Worker(app.worker)가 같은 방식으로 모델을 올린다."""
import os

import torch

from app.measure import DATA, Measurer

REQUIRED_FILES = ["background_cam1.png", "background_cam2.png", "background_cam3.png", "roi.json",
                  "product_db.npz", "products.json", "volume_model.joblib"]


def load_measurer() -> tuple[torch.device, Measurer]:
    missing = [f for f in REQUIRED_FILES if not (DATA / f).exists()]
    if missing:
        raise RuntimeError(f"app/data/에 모델·데이터 파일이 없습니다: {missing}\n"
                           "item-dimension-training으로 만든 outputs/를 app/data/로 복사하세요 (app/data/README.md 참고).")
    device = torch.device(os.getenv("DEVICE") or ("cuda" if torch.cuda.is_available() else "cpu"))
    return device, Measurer(device, threshold=float(os.getenv("KNOWN_THRESHOLD", "0.80")))
