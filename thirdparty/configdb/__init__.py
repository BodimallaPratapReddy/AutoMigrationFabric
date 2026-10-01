"""Fabric SQL configuration database helpers."""
"""Configuration database adapters.

Use :class:`ConfigDBRepository` for the current migration schema. The
``utils`` module remains for compatibility with the previous schema.
"""

from .repository import ConfigDBRepository

__all__ = ["ConfigDBRepository"]
