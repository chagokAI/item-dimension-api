"""폴리곤(라벨 segmentation 형식) 관련 계산."""
import numpy as np
from PIL import Image


def to_points(segmentation) -> np.ndarray:
    """[[x1, y1, x2, y2, ...]] 또는 [x1, y1, ...] -> (N, 2)"""
    flat = segmentation[0] if segmentation and isinstance(segmentation[0], (list, tuple)) else segmentation
    pts = np.asarray(flat, dtype=float).reshape(-1, 2)
    if len(pts) < 3:
        raise ValueError("폴리곤은 점이 3개 이상이어야 합니다.")
    return pts


def polygon_area(pts: np.ndarray) -> float:
    x, y = pts[:, 0], pts[:, 1]
    return 0.5 * abs(np.dot(x, np.roll(y, 1)) - np.dot(y, np.roll(x, 1)))


def bbox(pts: np.ndarray) -> list[float]:
    x0, y0 = pts.min(axis=0)
    x1, y1 = pts.max(axis=0)
    return [float(x0), float(y0), float(x1 - x0), float(y1 - y0)]


def convex_hull(pts: np.ndarray) -> np.ndarray:
    pts = sorted(map(tuple, pts))

    def half(seq):
        h = []
        for p in seq:
            while len(h) >= 2 and (h[-1][0] - h[-2][0]) * (p[1] - h[-2][1]) - (h[-1][1] - h[-2][1]) * (p[0] - h[-2][0]) <= 0:
                h.pop()
            h.append(p)
        return h

    return np.array(half(pts)[:-1] + half(pts[::-1])[:-1])


def min_area_rect(hull: np.ndarray) -> tuple[float, float]:
    """회전을 허용한 최소 면적 사각형의 (긴 변, 짧은 변)."""
    best = None
    for t in np.deg2rad(np.arange(0, 90, 1.0)):
        r = hull @ np.array([[np.cos(t), -np.sin(t)], [np.sin(t), np.cos(t)]])
        w, h = r.max(axis=0) - r.min(axis=0)
        if best is None or w * h < best[0]:
            best = (w * h, max(w, h), min(w, h))
    return best[1], best[2]


def crop_square(image: Image.Image, box: list[float], margin: float = 1.2) -> Image.Image:
    """bbox 중심 정사각형 크롭 (긴 변 x margin). 상품 DB도 같은 방식으로 만든다."""
    x, y, w, h = box
    cx, cy, half = x + w / 2, y + h / 2, max(w, h) * margin / 2
    return image.crop((round(cx - half), round(cy - half), round(cx + half), round(cy + half)))
