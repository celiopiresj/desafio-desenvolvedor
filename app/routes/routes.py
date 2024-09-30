from fastapi import APIRouter, UploadFile, File, Request
from fastapi.responses import JSONResponse, RedirectResponse
from typing import Dict
from services.file_service import save_file, get_files, get_files_by_name, get_files_by_upload_date, get_files_by_fields

router = APIRouter()


@router.get("/")
def read_root():
    """
    Fetch a single recipe by ID
    """
    return RedirectResponse("/docs")


@router.get("/files/", status_code=200)
async def list_files(page: int = 1, page_size: int = 10):
    results = await get_files(page, page_size)
    return {"files": results}


@router.post("/files/upload/", status_code=201)
async def upload_file(file: UploadFile = File(...)):
    result = await save_file(file)
    return JSONResponse(content=result)


@router.get("/files/filename/{filename}", status_code=200)
async def get_file_by_filename(filename: str, exact_filename_match: bool = True, include_content: bool = False, paginate: bool = True, page: int = 1, page_size: int = 10):
    results = await get_files_by_name(filename, include_content, page, page_size, paginate, exact_filename_match)
    return {"file": results}


@router.get("/files/upload_date/{upload_date}", status_code=200)
async def get_file_by_upload_date(upload_date: str, include_content: bool = False, paginate: bool = True, page: int = 1, page_size: int = 10):
    results = await get_files_by_upload_date(upload_date, include_content, page, page_size, paginate)
    return {"file": results}


@router.get("/files/fields", status_code=200)
async def get_file_by_fields(fields: Dict[str, str]):
    results = await get_files_by_fields(fields)
    return {"file": results}
