from fastapi import UploadFile, HTTPException
from io import StringIO, BytesIO
from bson import Regex
import pytz
from typing import Dict
import pandas as pd
from datetime import datetime
from database.database import db


def decode_content(contents: bytes) -> str:
    try:
        return contents.decode('utf-8')
    except UnicodeDecodeError:
        return contents.decode('ISO-8859-1')


async def check_file_exists(filename: str, db):
    if await db['files'].find_one({"Filename": filename}):
        raise HTTPException(
            status_code=400, detail="O arquivo já foi enviado anteriormente.")


def parse_csv_content(decoded_content: str) -> pd.DataFrame:
    initial_lines = decoded_content.splitlines()[:2]
    header = 0 if "RptDt" in initial_lines[0] else 1
    return pd.read_csv(StringIO(decoded_content), sep=";", header=header, na_filter=False, low_memory=False)


def parse_excel_content(file_content: bytes) -> pd.DataFrame:
    initial_lines = decode_content(file_content).splitlines()[:2]
    header = 0 if "RptDt" in initial_lines[0] else 1
    return pd.read_excel(BytesIO(file_content), header=header, engine='openpyxl')


async def save_data_to_mongo(db, data: list):
    collection = db['files']
    batch_size = 1000

    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        await collection.insert_many(batch)


def convert_to_date(value):
    try:
        if value is not None and value != "":

            date_value = pd.to_datetime(
                value, format="%Y-%m-%d", errors='coerce')

            if pd.isnull(date_value):
                date_value = pd.to_datetime(
                    value, format="%d/%m/%Y", errors='coerce')

            if pd.isnull(date_value):
                return ""
            return date_value
        return ""
    except Exception:
        # Se houver qualquer outro erro, retorna uma string vazia
        return ""


async def save_file(file: UploadFile):
    if not file:
        raise HTTPException(status_code=400, detail="Nenhum arquivo enviado.")

    await check_file_exists(file.filename, db)

    contents = await file.read()
    decoded_content = decode_content(contents)

    if file.content_type == "text/csv":
        try:
            df = parse_csv_content(decoded_content)
        except pd.errors.ParserError as e:
            raise HTTPException(
                status_code=400, detail=f"Erro ao analisar o arquivo CSV: {str(e)}")
    elif file.content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        try:
            df = parse_excel_content(contents)
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f"Erro ao analisar o arquivo Excel: {str(e)}")
    else:
        raise HTTPException(
            status_code=400, detail="Formato de arquivo não suportado.")

    df["Filename"] = file.filename

    local_tz = pytz.timezone("America/Sao_Paulo")
    date = datetime.now()
    date_string = date.astimezone(local_tz).strftime("%Y-%m-%d")
    df["Upload_date"] = pd.to_datetime(
        date_string, format="%Y-%m-%d", errors='coerce')

    # converter string para date
    columns_to_convert = [
        "RptDt", "XprtnDt", "TradgStartDt", "TradgEndDt",
        "DlvryNtceStartDt", "DlvryNtceEndDt", "OpngPosLmtDt", "CorpActnStartDt"
    ]

    df[columns_to_convert] = df[columns_to_convert].apply(
        lambda col: col.apply(convert_to_date))

    data = df.to_dict(orient='records')
    try:
        await save_data_to_mongo(db, data)
        return {"detail": "Arquivo enviado e dados salvos com sucesso."}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao salvar dados no MongoDB: {str(e)}")


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


async def paginate_files(page: int, page_size: int):

    pipeline = [
        {"$sort": {"RptDt": -1}},
        {"$skip": (page - 1) * page_size},
        {"$limit": page_size},
        {"$project": {"_id": 0, "Filename": 0, "Upload_date": 0}}
    ]

    result = await db["files"].aggregate(pipeline).to_list(length=page_size)

    total_documents = await db["files"].count_documents({})
    total_pages = (total_documents + page_size - 1) // page_size

    return {
        "current_page": page,
        "total_pages": total_pages,
        "page_size": page_size,
        "data": format_fields(result)
    }


async def get_distinct_fields(search_field: str, value: str, exact_match: bool = True):
    print(value)
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


async def paginate_file_by_name(filename: str, page: int, page_size: int, exact_match: bool):

    files = await get_distinct_fields(
        search_field="Filename",
        value=filename,
        exact_match=exact_match
    )

    for index, file in enumerate(files):

        pipeline = [
            {"$match": {"Filename": file["filename"]}},
            {"$sort": {"RptDt": -1}},
            {"$skip": (page - 1) * page_size},
            {"$limit": page_size},
            {"$project": {"_id": 0, "Filename": 0, "Upload_date": 0}}
        ]

        result = await db["files"].aggregate(pipeline).to_list(length=page_size)

        total_documents = await db["files"].count_documents({"Filename": file["filename"]})
        total_pages = (total_documents + page_size - 1) // page_size
        files[index]["total_pages"] = total_pages
        files[index]["data"] = format_fields(result)

    return {
        "files_found": len(files),
        "current_page": page,
        "page_size": page_size,
        "filename_search": filename,
        "result": files
    }


async def filter_files_by_name(filename: str, exact_match: bool):

    files = await get_distinct_fields(
        search_field="Filename",
        value=filename,
        exact_match=exact_match
    )

    for index, file in enumerate(files):
        pipeline = [
            {"$match": {"Filename": file["filename"]}},
            {"$project": {"_id": 0, "Filename": 0, "Upload_date": 0}}
        ]

        result = await db["files"].aggregate(pipeline).to_list()
        files[index]["data"] = format_fields(result)

    return {
        "files_found": len(files),
        "filename_search": filename,
        "result": files
    }


async def paginate_file_by_upload_date(upload_date: str, page: int, page_size: int):

    files = await get_distinct_fields(
        search_field="Upload_date",
        value=upload_date
    )

    for index, file in enumerate(files):

        pipeline = [
            {"$match": {"Filename": file["filename"]}},
            {"$sort": {"RptDt": -1}},
            {"$skip": (page - 1) * page_size},
            {"$limit": page_size},
            {"$project": {"_id": 0, "Filename": 0, "Upload_date": 0}}
        ]

        result = await db["files"].aggregate(pipeline).to_list(length=page_size)

        total_documents = await db["files"].count_documents({"Filename": file["filename"]})
        total_pages = (total_documents + page_size - 1) // page_size
        files[index]["total_pages"] = total_pages
        files[index]["data"] = format_fields(result)

    return {
        "files_found": len(files),
        "current_page": page,
        "page_size": page_size,
        "upload_date_search": upload_date,
        "result": files
    }


async def filter_files_by_upload_date(upload_date: str):

    files = await get_distinct_fields(
        search_field="Upload_date",
        value=upload_date,
    )

    for index, file in enumerate(files):
        pipeline = [
            {"$match": {"Filename": file["filename"]}},
            {"$project": {"_id": 0, "Filename": 0, "Upload_date": 0}}
        ]

        result = await db["files"].aggregate(pipeline).to_list()
        files[index]["data"] = format_fields(result)

    return {
        "files_found": len(files),
        "upload_date_search": upload_date,
        "result": files
    }


async def get_files(page: int, page_size: int):
    try:
        return await paginate_files(page, page_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
        raise HTTPException(status_code=500, detail=str(e))


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
        raise HTTPException(status_code=500, detail=str(e))


async def get_files_by_fields(fields: Dict[str, str], page: int, page_size: int, paginate: bool):
    try:
        if paginate:
            result = await paginate_file_by_fields(fields, page, page_size)
        else:
            result = await filter_files_by_fields(fields)

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
