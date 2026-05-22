"""Connector factory functions.

Each factory wires a worker, dialect, and
Connection together and returns a ready-to-use
Connection instance.
"""
from .athena import make_athena, make_athena_async

__all__ = ["make_athena", "make_athena_async"]
