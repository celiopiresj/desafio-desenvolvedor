from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from controllers.controller import upload

router = APIRouter()

@router.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    result = await upload(file)
    return JSONResponse(content=result)
