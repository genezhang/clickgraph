import json, os, time, uuid
"""Run one SQL statement on the Spark worker (`worker.py`) through its file
queue `$CG_SPARK_WORK/q`."""
Q = os.path.join(os.environ.get("CG_SPARK_WORK", "."), "q")
def run(sql, timeout=120):
    i = f"{time.time_ns()}_{uuid.uuid4().hex[:6]}"
    tmp = os.path.join(Q, i + ".part")
    open(tmp, "w").write(sql)
    os.chmod(tmp, 0o666)
    os.rename(tmp, os.path.join(Q, i + ".sql"))
    out = os.path.join(Q, i + ".out.json")
    t = time.time()
    while not os.path.exists(out):
        if time.time() - t > timeout:
            return {"error": "TIMEOUT"}
        time.sleep(0.02)
    r = json.load(open(out)); os.remove(out)
    return r
if __name__ == "__main__":
    import sys
    print(json.dumps(run(sys.argv[1])))
