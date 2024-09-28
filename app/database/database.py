import logging
import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://mongodb:27017")
db = client.financehub

logging.basicConfig(level=logging.INFO, format="%(levelname)s:     %(message)s")
logger = logging.getLogger()

async def startup_db():
    try:
        await client.server_info()
        logger.info("Conexão com MongoDB estabelecida com sucesso!")

        if 'files' not in await db.list_collection_names():
            await db.create_collection('files')
            logger.info("Coleção 'files' criada com sucesso.")
        else:
             logger.info("A coleção 'files' já existe.")
    except Exception as e:
        logger.error(f"Erro ao conectar ao MongoDB: {e}")

async def shutdown_db():
    client.close()