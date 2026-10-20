"""입고 이미지(cam 1~3) -> 아는 물건이면 실측값, 처음 보는 물건이면 추정값.

0. 물체 영역: 배경 차분 (app.segment)
1. 상품 식별: DINOv2-small 임베딩 + 등록 상품 DB와 코사인 유사도 (numpy)
2. 처음 보는 물건: 물체 영역 기하 특징 -> 치수 회귀 모델 (Validation 라벨로 학습)

물체 영역(shape)은 (윤곽 점들 (N, 2), 면적) 튜플. 라벨 폴리곤이든 배경 차분 결과든 같은 형태로 다룬다.
"""
import json
from pathlib import Path

import joblib
import numpy as np
import torch
from PIL import Image
from transformers import AutoImageProcessor, AutoModel

from app import geometry
from app.segment import BackgroundSegmenter

DATA = Path(__file__).resolve().parent / "data"
DINO = "facebook/dinov2-small"
FRAME_SIZE = (1920, 1080)  # 데이터셋 카메라 원본 해상도

Shape = tuple[np.ndarray, float]


def polygon_shape(segmentation) -> Shape:
    """라벨 segmentation -> shape"""
    pts = geometry.to_points(segmentation)
    return pts, geometry.polygon_area(pts)


def shape_features(shape: Shape) -> list[float]:
    """shape 하나 -> 7개 특징 (bbox 가로/세로, 면적, 회전 사각형 긴/짧은 변, solidity, 종횡비)."""
    pts, area = shape
    _, _, w, h = geometry.bbox(pts)
    area = max(area, 1.0)
    hull = geometry.convex_hull(pts)
    long_, short = geometry.min_area_rect(hull)
    short = max(short, 1.0)
    solidity = area / max(geometry.polygon_area(hull), 1.0)
    return [np.log(max(w, 1.0)), np.log(max(h, 1.0)), np.log(area), np.log(long_), np.log(short), solidity, np.log(long_ / short)]


def volume_features(shapes: list[Shape]) -> list[float]:
    """여러 뷰 shape -> 뷰별 7개 특징을 이어 붙임 (3장이면 21개, 6장이면 42개)."""
    return [v for s in shapes for v in shape_features(s)]


class Embedder:
    def __init__(self, device: torch.device):
        self.processor = AutoImageProcessor.from_pretrained(DINO)
        self.model = AutoModel.from_pretrained(DINO).to(device).eval()
        self.device = device

    @torch.inference_mode()
    def __call__(self, images: list[Image.Image]) -> np.ndarray:
        inputs = self.processor(images=images, return_tensors="pt").to(self.device)
        e = self.model(**inputs).pooler_output.cpu().numpy()
        return e / np.linalg.norm(e, axis=1, keepdims=True)


class Measurer:
    def __init__(self, device: torch.device, threshold: float = 0.80):
        self.embed = Embedder(device)
        self.segment = BackgroundSegmenter()
        self.threshold = threshold
        db = np.load(DATA / "product_db.npz")
        self.db_emb, self.db_index = db["embeddings"], db["product_index"]
        self.products = json.loads((DATA / "products.json").read_text(encoding="utf-8"))
        self.volume_model = joblib.load(DATA / "volume_model.joblib")

    def identify(self, query_emb: np.ndarray) -> tuple[int, float]:
        """쿼리 3장 각각에서 상품별 최대 유사도를 구해 평균 -> (1위 상품 번호, 점수)"""
        sims = query_emb @ self.db_emb.T
        n = len(self.products)
        per_product = np.full((len(query_emb), n), -1.0)
        for p in range(n):
            per_product[:, p] = sims[:, self.db_index == p].max(axis=1)
        scores = per_product.mean(axis=0)
        best = int(np.argmax(scores))
        return best, float(scores[best])

    def estimate_dims(self, shapes: list[Shape]) -> list[float]:
        pred = np.exp(self.volume_model.predict([volume_features(shapes)])[0])
        return sorted(pred.tolist(), reverse=True)

    def __call__(self, images: list[Image.Image]) -> dict:
        """images: cam1, cam2, cam3 순서. 반드시 카메라 원본 해상도(1920x1080)
        (배경·ROI·부피 모델이 모두 원본 해상도 좌표 기준이라 해상도가 다르면 결과가 틀어짐)."""
        for cam, img in enumerate(images, start=1):
            if img.size != FRAME_SIZE:
                raise ValueError(f"cam{cam} 이미지 해상도가 {img.size[0]}x{img.size[1]}입니다. "
                                 f"{FRAME_SIZE[0]}x{FRAME_SIZE[1]} 원본 그대로 보내야 합니다.")
        return self.measure_shapes(images, [self.segment(img, cam) for cam, img in enumerate(images, start=1)])

    def measure_shapes(self, images: list[Image.Image], shapes: list[Shape]) -> dict:
        crops = [geometry.crop_square(img, geometry.bbox(pts)) for img, (pts, _) in zip(images, shapes)]
        best, score = self.identify(self.embed(crops))

        if score >= self.threshold:
            p = self.products[best]
            status, kan_code, name, dims, size_type = "known", p["kan_code"], p["name"], p["dims_cm"], "measured"
        else:
            # 처음 보는 물건: 부피만 추정, KAN 코드·상품명은 null
            status, kan_code, name, dims, size_type = "unknown", None, None, self.estimate_dims(shapes), "estimated"

        w, d, h = dims
        return {
            "status": status,
            "kan_code": kan_code,
            "product_name": name,
            "size": {
                "type": size_type,
                "w_cm": round(w, 1),
                "d_cm": round(d, 1),
                "h_cm": round(h, 1),
                "volume_cm3": round(w * d * h, 1),
            },
        }
