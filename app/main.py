from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from routes.routes import router as routes
from database.database import startup_db, shutdown_db
# from config.database import create_collection

app = FastAPI(title="FinanceHub API")

app.include_router(routes)


@app.on_event("startup")
async def startup_event():
    await startup_db()


@app.on_event("shutdown")
async def shutdown_event():
    await shutdown_db()


@app.get("/")
def read_root():
    """
    Fetch a single recipe by ID
    """
    return RedirectResponse("/redoc")
