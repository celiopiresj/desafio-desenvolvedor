from fastapi import UploadFile, HTTPException
from io import StringIO, BytesIO 
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
        raise HTTPException(status_code=400, detail="O arquivo já foi enviado anteriormente.")

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


async def save_file(file: UploadFile):
    if not file:
        raise HTTPException(status_code=400, detail="Nenhum arquivo enviado.")
    
    # Verificar se o arquivo já foi enviado anteriormente
    await check_file_exists(file.filename, db)

    # Leitura e decodificação do conteúdo do arquivo
    contents = await file.read()
    decoded_content = decode_content(contents)

    # Processamento com base no tipo de arquivo
    if file.content_type == "text/csv":
        try:
            df = parse_csv_content(decoded_content)
        except pd.errors.ParserError as e:
            raise HTTPException(status_code=400, detail=f"Erro ao analisar o arquivo CSV: {str(e)}")
    elif file.content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": 
        try:
            df = parse_excel_content(contents)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Erro ao analisar o arquivo Excel: {str(e)}")
    else:
        raise HTTPException(status_code=400, detail="Formato de arquivo não suportado.")
    
   # Converte os dados e salva no MongoDB
    df["Filename"] = file.filename
    df["Upload_date"] = datetime.now().strftime("%Y-%m-%d")
    data = df.to_dict(orient='records')
    try:
        await save_data_to_mongo(db, data)
        return {"detail": "Arquivo enviado e dados salvos com sucesso."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao salvar dados no MongoDB: {str(e)}")  

async def paginate(page: int, page_size: int):
    pipeline = [ 
        {"$sort": {"RptDt": -1}},      
        {"$skip": (page - 1) * page_size},
        {"$limit": page_size},      
        {"$project": {"_id": 0, "Filename": 0, "Upload_date": 0}} 
    ]

    result =  db["files"].aggregate(pipeline)

    total_documents = await  db["files"].count_documents({})
    total_pages = (total_documents + page_size - 1) // page_size
    
    return {
        "current_page": page,
        "total_pages": total_pages,
        "page_size": page_size,
        "data": await result.to_list(length=page_size)
    }

async def get_files(page: int, page_size: int):
    try:
        #paginar consulta
        result = await paginate(page, page_size)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 