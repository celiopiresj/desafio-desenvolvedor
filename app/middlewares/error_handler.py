from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError


def error_handler(e: Exception, message: str):
    # Captura erros do MongoDB (motor)
    if isinstance(e, PyMongoError):
        raise HTTPException(
            status_code=500, detail=f"Erro do MongoDB: {str(e)}")

    # Captura exceções HTTP já lançadas
    elif isinstance(e, HTTPException):
        raise e

    # Captura quaisquer outras exceções
    else:
        raise HTTPException(
            status_code=500, detail=f"{message}: {str(e)}")
