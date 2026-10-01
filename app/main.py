from fastapi import FastAPI
from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str


app = FastAPI(title="Fabric API")


@app.get("/health", tags=["health"])
def health() -> HealthResponse:
    return HealthResponse(status="ok")
