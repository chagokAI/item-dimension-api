"""배경 차분으로 물체 영역 찾기 (고정 카메라 전제).

cam별 '빈 바닥' 배경(app/data/background_cam{1,2,3}.png)과 비교해서 달라진 영역 중 가장 큰 덩어리를 물체로 본다.
- 촬영 회차마다 벽·커튼 위치와 밝기가 조금씩 달라서, 물체가 놓이는 바닥 영역(roi.json)만 보고
  그 안의 밝기를 배경에 맞춘 뒤 비교한다.
- 계산은 절반 해상도에서 하고, 결과 좌표는 원본 해상도로 되돌린다.
"""
import json
from pathlib import Path

import numpy as np
from PIL import Image, ImageFilter
from scipy import ndimage

BG_SCALE = 0.5
DATA = Path(__file__).resolve().parent / "data"


class BackgroundSegmenter:
    def __init__(self, threshold: float = 20.0):  # Sample 240장 기준 라벨 폴리곤과 IoU가 가장 높았던 값
        self.threshold = threshold
        self.backgrounds = {
            cam: np.asarray(Image.open(DATA / f"background_cam{cam}.png").convert("RGB"), dtype=np.float32)
            for cam in (1, 2, 3)
        }
        # 원본 좌표 [x0, y0, x1, y1] -> 절반 해상도 정수 좌표
        rois = json.loads((DATA / "roi.json").read_text())
        self.rois = {int(c): [round(v * BG_SCALE) for v in r] for c, r in rois.items()}

    def mask(self, image: Image.Image, cam: int) -> np.ndarray:
        bg = self.backgrounds[cam]
        x0, y0, x1, y1 = self.rois[cam]
        small = image.convert("RGB").resize((bg.shape[1], bg.shape[0]), Image.BILINEAR).filter(ImageFilter.GaussianBlur(1.5))
        img = np.asarray(small, dtype=np.float32)[y0:y1, x0:x1]
        ref = bg[y0:y1, x0:x1]
        # 밝기 보정: 채널별 중앙값을 배경에 맞춤 (물체는 ROI의 일부라 중앙값은 바닥 기준)
        img = img * (np.median(ref, axis=(0, 1)) / np.maximum(np.median(img, axis=(0, 1)), 1.0))
        diff = np.abs(img - ref).max(axis=2)

        m = diff > self.threshold
        m = ndimage.binary_opening(m, iterations=2)
        m = ndimage.binary_closing(m, iterations=4)
        m = ndimage.binary_fill_holes(m)
        labels, n = ndimage.label(m)
        if n == 0:
            raise ValueError(f"cam{cam} 이미지에서 물체를 찾지 못했습니다.")
        sizes = ndimage.sum(m, labels, range(1, n + 1))

        full = np.zeros(bg.shape[:2], dtype=bool)
        full[y0:y1, x0:x1] = labels == (int(np.argmax(sizes)) + 1)
        return full

    def __call__(self, image: Image.Image, cam: int) -> tuple[np.ndarray, float]:
        """-> (윤곽 점들 (N, 2) 원본 좌표, 면적 원본 픽셀²)"""
        m = self.mask(image, cam)
        edge = m & ~ndimage.binary_erosion(m)
        ys, xs = np.nonzero(edge)
        pts = np.stack([xs, ys], axis=1).astype(float) / BG_SCALE
        return pts, float(m.sum()) / BG_SCALE ** 2
