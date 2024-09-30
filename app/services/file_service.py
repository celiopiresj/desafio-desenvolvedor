from fastapi import UploadFile, HTTPException
from io import StringIO, BytesIO
from bson import Regex
import pytz
from typing import Dict
import pandas as pd
from datetime import datetime
from database.database import db
from model.model import FilterParams
from middlewares.error_handler import error_handler


def _decode_content(contents: bytes) -> str:
    try:
        return contents.decode('utf-8')
    except UnicodeDecodeError:
        return contents.decode('ISO-8859-1')


async def _check_file_exists(filename: str, db):
    file_exists = await db['files'].find_one({"Filename": filename})
    if file_exists:
        raise HTTPException(
            status_code=400, detail="O arquivo já foi enviado anteriormente.")


def _parse_csv_content(decoded_content: str) -> pd.DataFrame:
    initial_lines = decoded_content.splitlines()[:2]
    header = 0 if "RptDt" in initial_lines[0] else 1
    return pd.read_csv(StringIO(decoded_content), sep=";", header=header, na_filter=False, low_memory=False)


def _parse_excel_content(file_content: bytes) -> pd.DataFrame:
    initial_lines = _decode_content(file_content).splitlines()[:2]
    header = 0 if "RptDt" in initial_lines[0] else 1
    return pd.read_excel(BytesIO(file_content), header=header, engine='openpyxl')


def _parse_file_content(file: UploadFile, decoded_content: str, contents: bytes) -> pd.DataFrame:
    if file.content_type == "text/csv":
        return _parse_csv_content(decoded_content)
    elif file.content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        return _parse_excel_content(contents)
    else:
        raise HTTPException(
            status_code=400, detail="Formato de arquivo não suportado.")


async def _save_data_to_mongo(db, data: list):
    collection = db['files']
    batch_size = 1000

    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        await collection.insert_many(batch)


async def _save_file_to_db(db, data: list):
    try:
        await _save_data_to_mongo(db, data)
    except Exception as e:
        error_handler(e, "Erro ao salvar dados no MongoDB")


def convert_to_date(value):
    try:
        if value and value != "":
            date_value = pd.to_datetime(
                value, format="%Y-%m-%d", errors='coerce')
            if pd.isnull(date_value):
                date_value = pd.to_datetime(
                    value, format="%d/%m/%Y", errors='coerce')
            return "" if pd.isnull(date_value) else date_value
        return ""
    except Exception:
        return ""


def _convert_columns_to_date(df: pd.DataFrame, columns_to_convert: list):
    df[columns_to_convert] = df[columns_to_convert].apply(
        lambda col: col.apply(convert_to_date))


def _get_current_date() -> pd.Timestamp:
    local_tz = pytz.timezone("America/Sao_Paulo")
    date_string = datetime.now().astimezone(local_tz).strftime("%Y-%m-%d")
    return pd.to_datetime(date_string, format="%Y-%m-%d", errors='coerce')


async def save_file(file: UploadFile):
    if not file:
        raise HTTPException(status_code=400, detail="Nenhum arquivo enviado.")

    await _check_file_exists(file.filename, db)

    contents = await file.read()
    decoded_content = _decode_content(contents)
    df = _parse_file_content(file, decoded_content, contents)

    df["Filename"] = file.filename
    df["Upload_date"] = _get_current_date()

    _convert_columns_to_date(df, columns_to_convert=[
        "RptDt", "XprtnDt", "TradgStartDt", "TradgEndDt",
        "DlvryNtceStartDt", "DlvryNtceEndDt", "OpngPosLmtDt", "CorpActnStartDt"
    ])

    data = df.to_dict(orient='records')
    await _save_file_to_db(db, data)

    return {"detail": "Arquivo enviado e dados salvos com sucesso."}


def format_date(field_name, value: str):
    date_fields = [
        "Upload_date", "RptDt", "XprtnDt", "TradgStartDt", "TradgEndDt",
        "DlvryNtceStartDt", "DlvryNtceEndDt", "OpngPosLmtDt", "CorpActnStartDt"
    ]

    if field_name in date_fields:
        result = convert_to_date(value)

        if result == "":
            return value
        return result
    else:
        return value


def format_fields(data_list):
    date_fields = [
        "Upload_date", "RptDt", "XprtnDt", "TradgStartDt", "TradgEndDt",
        "DlvryNtceStartDt", "DlvryNtceEndDt", "OpngPosLmtDt", "CorpActnStartDt"
    ]

    for item in data_list:
        for field in date_fields:
            if item.get(field) not in ("", None):
                item[field] = item[field].strftime('%Y-%m-%d')
            else:
                item[field] = ""

    return data_list


async def get_distinct_fields(search_field: str, value: str, exact_match: bool = True):
    try:
        if exact_match:
            match_stage = {search_field: format_date(search_field, value)}
        else:
            regex = Regex(f"^{value}", "i")
            match_stage = {search_field: {"$regex": regex}}

        pipeline = [
            {"$match": match_stage},
            {"$sort": {"Upload_date": -1}},
            {"$group": {"_id": "$Filename", "upload_date": {"$first": "$Upload_date"}}},
            {"$project": {"filename": "$_id", "_id": 0, "upload_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$upload_date"
                }
            }}}
        ]

        result = await db["files"].aggregate(pipeline).to_list()
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao buscar campos distintos: {str(e)}")


async def _get_files_by_field(field: str, value: str, exact_match: bool = True):
    try:
        return await get_distinct_fields(
            search_field=field,
            value=value,
            exact_match=exact_match
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao buscar arquivos por campo: {str(e)}")


def _build_pagination_pipeline(filename: str, page: int, page_size: int):
    return [
        {"$match": {"Filename": filename}},
        {"$sort": {"RptDt": -1}},
        {"$skip": (page - 1) * page_size},
        {"$limit": page_size},
        {"$project": {"_id": 0, "Filename": 0, "Upload_date": 0}}
    ]


async def _get_aggregated_data(pipeline: list, length: int = None):
    try:
        return await db["files"].aggregate(pipeline).to_list(length=length)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao agregar dados: {str(e)}")


async def _count_documents(filename: str = None):
    try:
        query = {}
        if filename:
            query["Filename"] = filename
        return await db["files"].count_documents(query)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao contar documentos: {str(e)}")


def _calculate_total_pages(total_documents: int, page_size: int):
    return (total_documents + page_size - 1) // page_size


async def _paginate_and_format_files(files: list, page: int, page_size: int):
    try:
        for index, file in enumerate(files):
            pipeline = _build_pagination_pipeline(
                file["filename"], page, page_size)
            result = await _get_aggregated_data(pipeline, page_size)

            total_documents = await _count_documents(file["filename"])
            total_pages = _calculate_total_pages(total_documents, page_size)

            files[index]["total_pages"] = total_pages
            files[index]["data"] = format_fields(result)

        return {
            "files_found": len(files),
            "current_page": page,
            "page_size": page_size,
            "result": files
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao paginar e formatar arquivos: {str(e)}")


def _build_filter_pipeline(filename: str):
    return [
        {"$match": {"Filename": filename}},
        {"$project": {"_id": 0, "Filename": 0, "Upload_date": 0}}
    ]


async def _format_filtered_files(files: list):
    try:
        for index, file in enumerate(files):
            pipeline = _build_filter_pipeline(file["filename"])
            result = await _get_aggregated_data(pipeline)

            files[index]["data"] = format_fields(result)

        return {
            "files_found": len(files),
            "result": files
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao formatar arquivos filtrados: {str(e)}")


async def paginate_files(page: int, page_size: int):

    pipeline = [
        {"$sort": {"RptDt": -1}},
        {"$skip": (page - 1) * page_size},
        {"$limit": page_size},
        {"$project": {"_id": 0, "Filename": 0, "Upload_date": 0}}
    ]

    result = await _get_aggregated_data(pipeline, page_size)
    total_documents = await _count_documents()
    total_pages = _calculate_total_pages(total_documents, page_size)

    return {
        "current_page": page,
        "total_pages": total_pages,
        "page_size": page_size,
        "data": format_fields(result)
    }


async def paginate_history_files(page: int, page_size: int):

    pipeline = [
        {"$sort": {"Upload_date": -1}},
        {"$group": {"_id": "$Filename", "upload_date": {"$first": "$Upload_date"}}},
        {"$project": {"filename": "$_id", "_id": 0, "upload_date": {
            "$dateToString": {
                "format": "%Y-%m-%d",
                "date": "$upload_date"
            }
        }}}
    ]

    result = await _get_aggregated_data(pipeline, page_size)

    total_count_result = await db["files"].aggregate([
        {"$group": {"_id": "$Filename"}},
        {"$count": "total"}
    ]).to_list(length=None)

    total_documents = total_count_result[0]["total"] if total_count_result else 0
    total_pages = _calculate_total_pages(total_documents, page_size)

    return {
        "current_page": page,
        "total_pages": total_pages,
        "page_size": page_size,
        "data": result
    }


async def paginate_file_by_name(filename: str, page: int, page_size: int, exact_match: bool):
    files = await _get_files_by_field("Filename", filename, exact_match)
    return await _paginate_and_format_files(files, page, page_size)


async def filter_files_by_name(filename: str, exact_match: bool):
    files = await _get_files_by_field("Filename", filename, exact_match)
    return await _format_filtered_files(files)


async def paginate_file_by_upload_date(upload_date: str, page: int, page_size: int):
    files = await _get_files_by_field("Upload_date", upload_date)
    return await _paginate_and_format_files(files, page, page_size)


async def filter_files_by_upload_date(upload_date: str):
    files = await _get_files_by_field("Upload_date", upload_date)
    return await _format_filtered_files(files)


async def paginate_file_by_fields(fields: Dict[str, str], page: int, page_size: int):
    try:
        match_stage = {k: format_date(
            k, v) for k, v in fields.items() if v not in ("", None)}

        pipeline = [
            {"$match": match_stage},
            {"$sort": {"RptDt": -1}},
            {"$skip": (page - 1) * page_size},
            {"$limit": page_size},
            {"$project": {"_id": 0, "Filename": 0, "Upload_date": 0}}
        ]

        result = await _get_aggregated_data(pipeline, page_size)
        total_documents = await _count_documents()
        total_pages = _calculate_total_pages(total_documents, page_size)

        return {
            "current_page": page,
            "total_pages": total_pages,
            "page_size": page_size,
            "data": format_fields(result)
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao paginar arquivos por campos: {str(e)}")


async def filter_files_by_fields(fields: Dict[str, str]):
    try:
        match_stage = {k: format_date(
            k, v) for k, v in fields.items() if v not in ("", None)}

        pipeline = [
            {"$match": match_stage},
            {"$project": {"_id": 0, "Filename": 0, "Upload_date": 0}}
        ]

        result = await _get_aggregated_data(pipeline)
        return {
            "data": format_fields(result)
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao filtrar arquivos por campos: {str(e)}")


async def get_files(page: int, page_size: int):
    try:
        return await paginate_files(page, page_size)
    except Exception as e:
        error_handler(e, "Erro ao buscar arquivos")


async def get_history_files(page: int, page_size: int):
    try:
        return await paginate_history_files(page, page_size)
    except Exception as e:
        error_handler(e, "Erro ao buscar historico de arquivos")


async def get_files_by_name(filename: str, include_content: bool, page: int, page_size: int,  paginate: bool, exact_match: bool):
    try:
        if include_content:
            if paginate:
                result = await paginate_file_by_name(filename, page, page_size, exact_match)
            else:
                result = await filter_files_by_name(filename, exact_match)
        else:
            files = await get_distinct_fields(
                search_field="Filename",

                value=filename,
                exact_match=exact_match
            )

            result = {
                "files_found": len(files),
                "filename_search": filename,
                "result": files
            }

        return result
    except Exception as e:
        error_handler(e, "Erro ao buscar arquivos por nome")


async def get_files_by_upload_date(upload_date: str, include_content: bool, page: int, page_size: int, paginate: bool):
    try:
        if include_content:
            if paginate:
                result = await paginate_file_by_upload_date(upload_date, page, page_size)
            else:
                result = await filter_files_by_upload_date(upload_date)
        else:
            files = await get_distinct_fields(
                search_field="Upload_date",
                value=upload_date
            )

            result = {
                "files_found": len(files),
                "upload_date_search": upload_date,
                "result": files
            }

        return result
    except Exception as e:
        error_handler(e, "Erro ao buscar arquivos por data de upload")


async def get_files_by_fields(fields: FilterParams):
    try:
        filters = {key: value for key, value in fields.dict().items()
                   if value is not None}

        # Extrair parâmetros de paginação
        paginate = filters.pop("paginate", True)
        page = filters.pop("page", 1)
        page_size = filters.pop("page_size", 10)

        if len(filters) == 0:
            raise HTTPException(
                status_code=400, detail="Nenhum campo foi passado.")

        if paginate:
            result = await paginate_file_by_fields(filters, page, page_size)
        else:
            result = await filter_files_by_fields(filters)

        return result
    except Exception as e:
        error_handler(e, "Erro ao buscar dados")


async def delete_file_by_filename(filename: str):
    try:
        exist = await db["files"].find_one({"Filename": filename})

        if exist is not None:
            result = await db["files"].delete_many({"Filename": filename})

            if result.deleted_count == 0:
                raise HTTPException(
                    status_code=404, detail="Documento não encontrado")

            return {"message": "Documento deletado com sucesso"}
        else:
            raise HTTPException(
                status_code=400, detail="Arquivo não existe.")

    except Exception as e:
        error_handler(e, "Erro ao deletar arquivo")
