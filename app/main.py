from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pathlib import Path

from app.migration.api import router as migration_router
from app.migration.connections_api import router as connections_router
from app.migration.workspaces_api import router as workspaces_router
import truststore
truststore.inject_into_ssl()

class HealthResponse(BaseModel):
    status: str


app = FastAPI(title="Fabric API")
app.include_router(migration_router)
app.include_router(connections_router)
app.include_router(workspaces_router)


@app.get("/health", tags=["health"])
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.get("/", include_in_schema=False)
def migration_ui() -> FileResponse:
    return FileResponse(Path(__file__).with_name("migration_ui.html"))
