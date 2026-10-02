"""Manage source connection records for the thin UI."""

import asyncio
import re
from typing import Literal

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, model_validator

from thirdparty.configdb.repository import ConfigDBRepository, DBConnectionCreate


router = APIRouter(prefix="/connections", tags=["connections"])


class ConnectionCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_type: Literal["ORACLE", "SAP_ECC"]
    connection_name: str = Field(min_length=1, max_length=200)
    host: str | None = None
    port: int | None = Field(default=None, ge=1, le=65535)
    dsn: str | None = None
    service_name: str | None = None
    sap_client: str | None = None
    username: str = Field(min_length=1)
    password: str = Field(min_length=1, repr=False)
    tags: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_endpoint(self) -> "ConnectionCreateRequest":
        self.connection_name = self.connection_name.strip()
        if not self.connection_name:
            raise ValueError("connection_name must be nonempty")
        for endpoint in (self.host, self.dsn, self.service_name):
            if endpoint and ("@" in endpoint or re.search(
                    r"(?i)(password|passwd|pwd|secret|token)\s*=", endpoint)):
                raise ValueError("endpoint fields must not contain credentials")
        if self.source_type == "ORACLE":
            if self.sap_client:
                raise ValueError("sap_client is only valid for SAP ECC")
            if bool(self.dsn) == bool(self.host and self.service_name):
                raise ValueError("provide Oracle DSN or both host and service name")
            if self.dsn and (self.host or self.service_name):
                raise ValueError("provide either Oracle DSN or host and service name")
            if self.dsn and self.port is not None:
                raise ValueError("port is part of the Oracle DSN")
        else:
            if self.dsn or self.service_name:
                raise ValueError("DSN and service name are only valid for Oracle")
            if not self.host or not self.port or not self.sap_client:
                raise ValueError("SAP ECC requires host, HTTPS port, and SAP client")
        return self

    def connection_details(self) -> dict:
        if self.source_type == "ORACLE":
            endpoint = ({"dsn": self.dsn} if self.dsn else
                        {"host": self.host, "serviceName": self.service_name,
                         "port": self.port or 1521})
            return {**endpoint, "username": self.username, "password": self.password}
        return {"host": self.host, "https_port": self.port,
                "sap_client": self.sap_client, "username": self.username,
                "password": self.password}


class ConnectionSummary(BaseModel):
    connection_name: str
    source_type: Literal["ORACLE", "SAP_ECC"]
    is_active: bool


@router.get("", response_model=list[ConnectionSummary])
async def list_connections() -> list[ConnectionSummary]:
    try:
        records = await asyncio.to_thread(lambda: ConfigDBRepository().list_connections())
    except Exception as exc:
        raise HTTPException(503, "Connection registry is unavailable") from exc
    return [ConnectionSummary(connection_name=item.connection_name,
                              source_type=item.source_type, is_active=item.is_active)
            for item in records if item.source_type in {"ORACLE", "SAP_ECC"}]


@router.post("", response_model=ConnectionSummary, status_code=status.HTTP_201_CREATED)
async def create_connection(request: ConnectionCreateRequest) -> ConnectionSummary:
    def save() -> bool:
        db = ConfigDBRepository()
        if db.get_connection(request.connection_name) is not None:
            return False
        db.insert_connection(DBConnectionCreate(
            source_type=request.source_type,
            connection_name=request.connection_name,
            connection_details=request.connection_details(),
            connection_tags=request.tags or None,
            is_active=True))
        return True

    try:
        created = await asyncio.to_thread(save)
    except Exception as exc:
        # A concurrent insert can win after the initial read. Detect that case
        # without disclosing SQL connection strings or driver diagnostics.
        try:
            exists = await asyncio.to_thread(
                lambda: ConfigDBRepository().get_connection(request.connection_name) is not None)
        except Exception:
            exists = False
        if exists:
            raise HTTPException(409, "Connection name already exists") from exc
        raise HTTPException(503, "Connection could not be created") from exc
    if not created:
        raise HTTPException(409, "Connection name already exists")
    return ConnectionSummary(connection_name=request.connection_name,
                             source_type=request.source_type, is_active=True)
