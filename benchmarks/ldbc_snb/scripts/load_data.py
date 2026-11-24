#!/usr/bin/env python3
"""
Load LDBC SNB data into ClickHouse.

Reads the CSV tree produced by ``scripts/download_data.sh`` (the LDBC datagen
Docker image) and streams every table into a running ClickHouse over HTTP.
No local ``clickhouse-client`` is required.

Usage:
    python3 scripts/load_data.py --scale-factor sf1
    python3 scripts/load_data.py --scale-factor sf1 \
        --url http://localhost:8123 --user default --password default
"""

from __future__ import annotations

import argparse
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"
DATAGEN_SUBDIR = Path("graphs") / "csv" / "interactive" / "composite-projected-fk"

# ClickHouse table -> directory under DATAGEN_SUBDIR. Datagen names a few
# directories after the concrete entity (City, Country, Company, University)
# while ClickHouse uses the abstract vertex type (Place, Organisation).
STATIC_TABLES = [
    ("Place", "static/Place"),
    ("Place_isPartOf_Place", "static/Place_isPartOf_Place"),
    ("Organisation", "static/Organisation"),
    ("Organisation_isLocatedIn_Place", "static/Organisation_isLocatedIn_Place"),
    ("Tag", "static/Tag"),
    ("TagClass", "static/TagClass"),
    ("Tag_hasType_TagClass", "static/Tag_hasType_TagClass"),
    ("TagClass_isSubclassOf_TagClass", "static/TagClass_isSubclassOf_TagClass"),
]

DYNAMIC_TABLES = [
    ("Person", "dynamic/Person"),
    ("Person_isLocatedIn_Place", "dynamic/Person_isLocatedIn_City"),
    ("Person_hasInterest_Tag", "dynamic/Person_hasInterest_Tag"),
    ("Person_workAt_Organisation", "dynamic/Person_workAt_Company"),
    ("Person_studyAt_Organisation", "dynamic/Person_studyAt_University"),
    ("Person_knows_Person", "dynamic/Person_knows_Person"),
    ("Forum", "dynamic/Forum"),
    ("Forum_hasModerator_Person", "dynamic/Forum_hasModerator_Person"),
    ("Forum_hasMember_Person", "dynamic/Forum_hasMember_Person"),
    ("Forum_hasTag_Tag", "dynamic/Forum_hasTag_Tag"),
    ("Post", "dynamic/Post"),
    ("Post_hasCreator_Person", "dynamic/Post_hasCreator_Person"),
    ("Post_isLocatedIn_Place", "dynamic/Post_isLocatedIn_Country"),
    ("Post_hasTag_Tag", "dynamic/Post_hasTag_Tag"),
    ("Forum_containerOf_Post", "dynamic/Forum_containerOf_Post"),
    ("Comment", "dynamic/Comment"),
    ("Comment_hasCreator_Person", "dynamic/Comment_hasCreator_Person"),
    ("Comment_isLocatedIn_Place", "dynamic/Comment_isLocatedIn_Country"),
    ("Comment_hasTag_Tag", "dynamic/Comment_hasTag_Tag"),
    ("Comment_replyOf_Post", "dynamic/Comment_replyOf_Post"),
    ("Comment_replyOf_Comment", "dynamic/Comment_replyOf_Comment"),
    ("Person_likes_Post", "dynamic/Person_likes_Post"),
    ("Person_likes_Comment", "dynamic/Person_likes_Comment"),
]

# The datagen Person CSV has scalar `language` and `email` fields; the schema
# stores arrays. Keep the CSV untouched and transform on insert.
PERSON_INSERT = """INSERT INTO ldbc.Person
(creationDate, id, firstName, lastName, gender, birthday, locationIP,
 browserUsed, speaks, email)
SELECT creationDate, id, firstName, lastName, gender, birthday, locationIP,
       browserUsed, splitByChar(';', language), splitByChar(';', email)
FROM input('creationDate Int64, id UInt64, firstName String, lastName String, gender String, birthday Int64, locationIP String, browserUsed String, language String, email String')
FORMAT CSVWithNames"""


class LoadError(RuntimeError):
    """A ClickHouse request failed or the generated data is incomplete."""


def run_http(
    url: str,
    params: dict[str, str],
    body: object = None,
    timeout: float = 600.0,
) -> str:
    endpoint = f"{url.rstrip('/')}/?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(endpoint, data=body, method="POST")
    if hasattr(body, "fileno"):
        request.add_header("Content-Length", str(os.fstat(body.fileno()).st_size))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace").strip()
        raise LoadError(detail or f"HTTP {exc.code}") from None
    except urllib.error.URLError as exc:
        raise LoadError(f"cannot reach {url}: {exc.reason}") from None


def apply_schema(url: str, params: dict[str, str], ddl: Path, timeout: float) -> None:
    text = "\n".join(line.split("--", 1)[0] for line in ddl.read_text().splitlines())
    for statement in text.split(";"):
        statement = statement.strip()
        if statement:
            run_http(url, dict(params, query=statement), timeout=timeout)


def row_count(url: str, params: dict[str, str], table: str, timeout: float) -> int:
    text = run_http(
        url,
        dict(params, query=f"SELECT count() FROM ldbc.{table} FORMAT TabSeparated"),
        timeout=timeout,
    )
    return int(text.strip())


def load_table(
    url: str,
    params: dict[str, str],
    table: str,
    directory: Path,
    timeout: float,
) -> int:
    files = sorted(directory.glob("part-*.csv")) or sorted(directory.glob("*.csv"))
    if not files:
        raise LoadError(f"no CSV files in {directory}")

    run_http(url, dict(params, query=f"TRUNCATE TABLE ldbc.{table}"), timeout=timeout)
    insert = PERSON_INSERT if table == "Person" else (
        f"INSERT INTO ldbc.{table} FORMAT CSVWithNames"
    )
    for path in files:
        with path.open("rb") as body:
            run_http(url, dict(params, query=insert), body, timeout)
    return row_count(url, params, table, timeout)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load LDBC SNB datagen CSV files into ClickHouse"
    )
    parser.add_argument("--scale-factor", "-s", default="sf1")
    parser.add_argument(
        "--data-dir",
        default=None,
        help="composite-projected-fk directory (default: data/<scale-factor>/...)",
    )
    parser.add_argument("--url", default=os.environ.get("CLICKHOUSE_URL", "http://localhost:8123"))
    parser.add_argument("--user", "-u", default=os.environ.get("CLICKHOUSE_USER", "default"))
    parser.add_argument(
        "--password", "-p", default=os.environ.get("CLICKHOUSE_PASSWORD", "default")
    )
    parser.add_argument(
        "--ddl", default=str(SCHEMA_DIR / "clickhouse_ddl.sql")
    )
    parser.add_argument("--timeout", type=float, default=600.0)
    args = parser.parse_args()

    data_dir = (
        Path(args.data_dir)
        if args.data_dir
        else SCHEMA_DIR.parent / "data" / args.scale_factor / DATAGEN_SUBDIR
    )
    if not data_dir.is_dir():
        print(f"ERROR: data directory not found: {data_dir}")
        print(f"Generate it first: ./scripts/download_data.sh {args.scale_factor}")
        return 1

    params = {
        "user": args.user,
        "password": args.password,
        "format_csv_delimiter": "|",
    }

    print("=" * 60)
    print("LDBC SNB data loader")
    print("=" * 60)
    print(f"Scale factor: {args.scale_factor}")
    print(f"Data:         {data_dir}")
    print(f"ClickHouse:   {args.url} (ldbc)")
    print(f"Schema:       {args.ddl}")
    print("=" * 60)

    try:
        apply_schema(args.url, params, Path(args.ddl), args.timeout)
        counts: dict[str, int] = {}
        for table, subdir in STATIC_TABLES + DYNAMIC_TABLES:
            print(f"Loading {table} ...", end=" ", flush=True)
            count = load_table(
                args.url, params, table, data_dir / subdir, args.timeout
            )
            counts[table] = count
            print(f"{count:,} rows")
        message_count = row_count(args.url, params, "Message", args.timeout)
    except (LoadError, OSError, ValueError) as exc:
        print(f"\nERROR: {exc}")
        return 1

    print("=" * 60)
    print(f"Message view: {message_count:,} rows")
    empty = [table for table, count in counts.items() if count == 0]
    if empty:
        print(f"ERROR: empty tables: {', '.join(empty)}")
        return 1
    print(f"Loaded {len(counts)} tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
