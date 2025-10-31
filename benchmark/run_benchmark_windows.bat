@echo off
REM Windows wrapper for ClickGraph benchmarking with proper UTF-8 encoding

REM Set UTF-8 encoding for Python
set PYTHONIOENCODING=utf-8

REM Run the benchmark command
python %*