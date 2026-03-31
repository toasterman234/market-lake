from __future__ import annotations
import argparse, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from market_lake.io.duckdb_conn import connect, run_sql_file
from market_lake.settings import Settings

def render_views(template_path: Path, root: Path) -> str:
    return template_path.read_text(encoding="utf-8").replace("{root}", str(root).replace("\\", "/"))

def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize market-lake DuckDB.")
    parser.add_argument("--root", default=None)
    args = parser.parse_args()
    settings = Settings.load()
    root = Path(args.root).resolve() if args.root else settings.root
    db_path = settings.duckdb_path
    init_dir = root / "duckdb" / "init"
    print(f"Root   : {root}")
    print(f"DuckDB : {db_path}")
    con = connect(db_path)
    try:
        run_sql_file(con, init_dir / "001_extensions.sql")
        print("  Extensions loaded")
        run_sql_file(con, init_dir / "002_schemas.sql")
        print("  Schemas created")
        views_sql = render_views(init_dir / "003_views.sql", root)
        for stmt in views_sql.split(";"):
            stmt = stmt.strip()
            if not stmt or stmt.startswith("--"):
                continue
            try:
                con.execute(stmt)
            except Exception as e:
                print(f"  View skipped (data not yet present): {e}")
        print("  Views registered")
        print(f"DuckDB initialized at {db_path}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
