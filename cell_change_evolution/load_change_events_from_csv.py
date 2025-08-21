import os
import sys
import logging
from pathlib import Path
import pandas as pd
import dotenv
from sqlalchemy import create_engine
import csv

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("csv-loader")

# Env
dotenv.load_dotenv()
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')
ROOT_DIRECTORY = os.getenv('ROOT_DIRECTORY')

# Determine candidate locations for CSVs
_cwd = Path.cwd()
_this_dir = Path(__file__).resolve().parent
_repo_root = _this_dir.parent  # project root (parent of cell_change_evolution)
_env_root = Path(ROOT_DIRECTORY) if ROOT_DIRECTORY else None

def find_csv(basename: str) -> Path:
    candidates = []
    if _env_root:
        candidates.append(_env_root / basename)
    candidates.extend([
        _cwd / basename,
        _repo_root / basename,
    ])
    for p in candidates:
        if p.exists():
            return p
    # Fallback to default under repo root
    return _repo_root / basename

LTE_CSV = find_csv('lte_cell_change_event.csv')
UMTS_CSV = find_csv('umts_cell_change_event.csv')


def engine():
    url = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(url)


def load_csv_to_table(csv_path: Path, table: str, date_columns: list):
    if not csv_path.exists():
        log.error(f"CSV not found: {csv_path}")
        return False

    # Use PostgreSQL COPY for performance and to avoid parameter limits
    eng = engine()
    try:
        # Extract header columns from CSV accurately
        with open(csv_path, 'r', newline='') as f:
            reader = csv.reader(f)
            header = next(reader)
        # Clean header names (remove BOM/whitespace and quotes if any)
        columns = [h.strip().lstrip('\ufeff').strip('"') for h in header]

        col_list = ', '.join([f'"{c}"' for c in columns])
        copy_sql = f"COPY public.{table} ({col_list}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"

        log.info(f"COPY into {table} from {csv_path} ...")
        raw = eng.raw_connection()
        try:
            with raw.cursor() as cur, open(csv_path, 'r') as f:
                cur.copy_expert(copy_sql, f)
            raw.commit()
        finally:
            raw.close()
        log.info("COPY completed")
        return True
    finally:
        eng.dispose()


def main():
    # LTE
    ok_lte = load_csv_to_table(
        LTE_CSV,
        table='lte_cell_change_event',
        date_columns=['date', 'created_at']
    )

    # UMTS
    ok_umts = load_csv_to_table(
        UMTS_CSV,
        table='umts_cell_change_event',
        date_columns=['date', 'created_at']
    )

    if not (ok_lte and ok_umts):
        sys.exit(1)


if __name__ == '__main__':
    main()
