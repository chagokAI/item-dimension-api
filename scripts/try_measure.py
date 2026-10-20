"""/measure 호출 예제: Sample 상품 폴더의 한 shot(cam 1~3 이미지 3장)을 보내고 라벨과 비교.

사용법: .venv/bin/python scripts/try_measure.py <원천 상품 폴더> [shot=2] [서버=http://127.0.0.1:8000]
예:    .venv/bin/python scripts/try_measure.py data/Sample/01.원천데이터/01_입고물품/01_가공식품/01_조미료/01010110_8809251219122
"""
import glob
import json
import sys

import requests

image_dir = sys.argv[1].rstrip("/")
shot = sys.argv[2] if len(sys.argv) > 2 else "2"
server = sys.argv[3] if len(sys.argv) > 3 else "http://127.0.0.1:8000"

images = [glob.glob(f"{image_dir}/*_{shot}_{cam}.jpg")[0] for cam in (1, 2, 3)]
result = requests.post(f"{server}/measure", files=[("images", open(p, "rb")) for p in images]).json()
print(json.dumps(result, ensure_ascii=False, indent=2))

# 라벨과 비교 (결과 확인용)
label = images[0].replace("01.원천데이터", "02.라벨링데이터").removesuffix(".jpg") + ".json"
at = json.load(open(label, encoding="utf-8"))["annotations"][0]["attributes"]
dims = sorted([at["length"], at["width"], at["height"]], reverse=True)
print(f"\n[라벨] {at['KAN_code']} {at['product_name']}  "
      f"{dims[0]} x {dims[1]} x {dims[2]} = {dims[0] * dims[1] * dims[2]:,.1f}cm³")
