from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from services.file_service import save_file, get_files

router = APIRouter()

@router.post("/upload/", status_code=201)
async def upload_file(file: UploadFile = File(...)):
    result = await save_file(file)
    return JSONResponse(content=result)

@router.get("/files/", status_code=200)
async def list_files(page: int = 1, page_size: int = 10):
    results = await get_files(page, page_size)
    return {"files": results}