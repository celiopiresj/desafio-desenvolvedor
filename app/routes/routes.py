from fastapi import APIRouter, UploadFile, File, Query
from fastapi.responses import JSONResponse, RedirectResponse
from model.model import FilterParams
from typing import Dict, Annotated
from services.file_service import save_file, get_files, get_files_by_name, get_files_by_upload_date, get_files_by_fields, delete_file_by_filename

from pydantic import BaseModel, Field
from typing_extensions import Annotated, Literal

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
async def get_file_by_fields(fields:  Annotated[FilterParams, Query()]):
    results = await get_files_by_fields(fields)
    return {"file": results}


@router.delete("/files/{filename}", status_code=204)
async def delete_file(filename: str):
    result = await delete_file_by_filename(filename)
    return {"file": result}
