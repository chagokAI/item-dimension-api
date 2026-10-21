import io
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, UploadFile
from PIL import Image, UnidentifiedImageError

from app.loader import load_measurer

models: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 서버 시작 시 한 번만 모델을 메모리에 올린다
    models["device"], models["measure"] = load_measurer()
    yield
    models.clear()


app = FastAPI(title="물류 입고 물품 부피 측정 API", lifespan=lifespan)


def read_image(upload: UploadFile) -> Image.Image:
    try:
        return Image.open(io.BytesIO(upload.file.read())).convert("RGB")
    except UnidentifiedImageError:
        raise HTTPException(status_code=400, detail="이미지 파일을 읽을 수 없습니다.")


@app.get("/health")
def health():
    return {"status": "ok", "device": str(models.get("device"))}


# 추론은 CPU/GPU 연산이라 async가 아닌 def로 둔다 (FastAPI가 스레드풀에서 실행)
@app.post("/measure")
def measure(images: list[UploadFile] = File(..., description="cam1, cam2, cam3 순서로 1920x1080 원본 이미지 3장")):
    """입고 이미지 → 아는 물건이면 실측값, 처음 보는 물건이면 추정값 (w ≥ d ≥ h)"""
    if len(images) != 3:
        raise HTTPException(status_code=400, detail="이미지는 cam1~3 순서로 3장이어야 합니다.")
    imgs = [read_image(f) for f in images]
    try:
        return models["measure"](imgs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
