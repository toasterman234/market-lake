from __future__ import annotations
from contextlib import contextmanager
from pathlib import Path
from typing import Generator
import duckdb

def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))

@contextmanager
def open_db(db_path: Path) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    con = connect(db_path)
    try:
        yield con
    finally:
        con.close()

def run_sql_file(con: duckdb.DuckDBPyConnection, sql_path: Path) -> None:
    con.execute(sql_path.read_text(encoding="utf-8"))

def query_df(con, sql: str):
    return con.execute(sql).df()
