from setuptools import setup, find_packages

setup(
    name="cg-schema",
    version="0.1.0",
    description="ClickGraph Schema Designer - ML-powered schema discovery",
    author="ClickGraph Team",
    author_email="team@clickgraph.dev",
    packages=find_packages(include=["cg_schema", "cg_schema.*"]),
    install_requires=[
        "clickhouse-connect>=0.6.0",
        "gliner>=0.2.0",
        "pyyaml>=6.0",
        "requests>=2.28.0",
        "rich>=13.0.0",
    ],
    entry_points={
        "console_scripts": [
            "cg-schema=cg_schema.cli:main",
        ],
    },
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
