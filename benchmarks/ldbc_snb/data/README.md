# LDBC SNB Data Directory

This directory contains downloaded LDBC SNB datasets.

## Download Data

```bash
# From the benchmarks/ldbc_snb directory:
./scripts/download_data.sh sf0.1
```

## Directory Structure

After download, you'll have:

```
data/
├── sf0.1/
│   ├── static/
│   │   ├── place_0_0.csv
│   │   ├── organisation_0_0.csv
│   │   ├── tag_0_0.csv
│   │   ├── tagclass_0_0.csv
│   │   └── ...
│   └── dynamic/
│       ├── person_0_0.csv
│       ├── post_0_0.csv
│       ├── comment_0_0.csv
│       ├── forum_0_0.csv
│       └── ...
└── sf1/
    └── ...
```

## Note

Data files are large and should NOT be committed to git.
This directory is in .gitignore.
