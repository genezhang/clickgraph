# LDBC SNB Data Directory

Generated datasets land here, one directory per scale factor. They are large and
ignored by git (`/benchmarks/ldbc_snb/data/sf*`). `mini_dataset.sql` is the one
tracked file besides this one: a few rows per table for smoke tests.

## Generate and load

From `benchmarks/ldbc_snb`:

```bash
bash ./scripts/download_data.sh sf1 --generate    # LDBC datagen in Docker
python3 scripts/load_data.py --scale-factor sf1   # needs ClickHouse up: see ../README.md
```

## Layout

```text
data/sf1/
├── .generated                      # marker: delete the directory to regenerate
└── graphs/csv/interactive/composite-projected-fk/
    ├── static/                     # Place, Organisation, Tag, TagClass and their edges
    │   └── Place/part-*.csv
    └── dynamic/                    # Person, Forum, Post, Comment and their edges
        └── Person/part-*.csv
```

The CSV files are `|`-delimited with a header row. Datagen runs as root inside its
container, so the generated files are owned by root; remove a scale factor with
`docker run --rm -v "$PWD/data:/d" --entrypoint rm clickhouse/clickhouse-server:25.8.11 -rf /d/sf1`.
