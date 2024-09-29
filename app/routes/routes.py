from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from services.file_service import save_file, get_files, get_files_by_name

router = APIRouter()

@router.post("/upload/", status_code=201)
async def upload_file(file: UploadFile = File(...)):
    result = await save_file(file)
    return JSONResponse(content=result)

@router.get("/files/", status_code=200)
async def list_files(page: int = 1, page_size: int = 10):
    results = await get_files(page, page_size)
    return {"files": results}

@router.get("/files/filename/{filename}", status_code=200)
async def get_file_by_filename(filename: str, page: int = 1, page_size: int = 10, paginate: bool = True, exact_match: bool = True):
    results = await get_files_by_name(filename, page, page_size, paginate, exact_match)
    return {"file": results}