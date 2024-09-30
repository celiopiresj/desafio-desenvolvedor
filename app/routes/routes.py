from fastapi import APIRouter, UploadFile, File, Query
from fastapi.responses import JSONResponse, RedirectResponse
from model.model import FilterParams
from typing import Dict, Annotated
from services.file_service import save_file, get_files, get_files_by_name, get_files_by_upload_date, get_files_by_fields, delete_file_by_filename

from pydantic import BaseModel, Field
from typing_extensions import Annotated

router = APIRouter(tags=["Arquivos"])


@router.get("/")
def read_root():
    """Redireciona para a documentação da API."""
    return RedirectResponse("/docs")


@router.get("/files/", status_code=200)
async def list_files(
    page: int = Query(1, description="Número da página a ser exibida."),
    page_size: int = Query(10, description="Número de arquivos por página.")
):
    """Lista todos os arquivos.

    - **page**: Número da página a ser exibida.
    - **page_size**: Número de arquivos por página.
    """
    results = await get_files(page, page_size)
    return {"files": results}


@router.post("/files/upload/", status_code=201)
async def upload_file(
    file: UploadFile = File(..., description="O arquivo a ser enviado.")
):
    """Faz o upload de um arquivo.

    - **file**: O arquivo a ser enviado.
    """
    result = await save_file(file)
    return JSONResponse(content=result)


@router.get("/files/filename/{filename}", status_code=200)
async def get_file_by_filename(
    filename: str,
    exact_filename_match: bool = Query(
        True, description="Se verdadeiro, busca por correspondência exata."),
    include_content: bool = Query(
        False, description="Se verdadeiro, inclui o conteúdo do arquivo na resposta."),
    paginate: bool = Query(
        True, description="Se verdadeiro, aplica paginação aos resultados."),
    page: int = Query(1, description="Número da página a ser exibida."),
    page_size: int = Query(10, description="Número de arquivos por página.")
):
    """Obtém um arquivo pelo nome.

    - **filename**: O nome do arquivo a ser buscado.
    """
    results = await get_files_by_name(filename, include_content, page, page_size, paginate, exact_filename_match)
    return {"file": results}


@router.get("/files/upload_date/{upload_date}", status_code=200)
async def get_file_by_upload_date(
    upload_date: str,
    include_content: bool = Query(
        False, description="Se verdadeiro, inclui o conteúdo do arquivo na resposta."),
    paginate: bool = Query(
        True, description="Se verdadeiro, aplica paginação aos resultados."),
    page: int = Query(1, description="Número da página a ser exibida."),
    page_size: int = Query(10, description="Número de arquivos por página.")
):
    """Obtém arquivos pela data de upload.

    - **upload_date**: A data de upload no formato desejado (ex: YYYY-MM-DD).
    """
    results = await get_files_by_upload_date(upload_date, include_content, page, page_size, paginate)
    return {"file": results}


@router.get("/files/fields", status_code=200)
async def get_file_by_fields(
    fields: Annotated[FilterParams, Query(
        description="Parâmetros de filtro para busca.")]
):
    """Obtém arquivos com base em campos filtrados."""
    results = await get_files_by_fields(fields)
    return {"file": results}


@router.delete("/files/{filename}", status_code=204)
async def delete_file(filename: str):
    """Deleta um arquivo pelo nome.

    - **filename**: O nome do arquivo a ser deletado.
    """
    result = await delete_file_by_filename(filename)
    return {"file": result}
