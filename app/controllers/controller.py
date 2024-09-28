from fastapi import UploadFile
from utils.utils import save_file

async def upload(file: UploadFile):
    result = await save_file(file)
    return result