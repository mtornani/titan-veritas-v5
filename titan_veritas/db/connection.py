import os
import sqlite3
from typing import Optional

_connection: Optional[sqlite3.Connection] = None


def get_db(path: Optional[str] = None) -> sqlite3.Connection:
    """Return a singleton SQLite connection. Thread-safe for single-writer workloads."""
    global _connection
    if _connection is not None:
        return _connection

    db_path = path or os.environ.get("TITAN_DB_PATH", "titan_veritas.db")
    _connection = sqlite3.connect(db_path, check_same_thread=False)
    _connection.row_factory = sqlite3.Row
    _connection.execute("PRAGMA journal_mode=WAL")
    _connection.execute("PRAGMA foreign_keys=ON")
    return _connection


def close_db():
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None
