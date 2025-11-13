# Development Environment Checklist

**Always run this checklist before starting a development session!**

## ✅ Pre-Development Checklist

### 1. Clean Docker Environment
```powershell
# Check for old ClickGraph/Brahmand containers
docker ps -a | Select-String "clickgraph|brahmand"

# If found, stop and remove them
docker stop brahmand
docker rm brahmand

# Optional: Remove old images
docker images | Select-String "clickgraph|brahmand"
docker rmi clickgraph-brahmand  # if you want a truly clean start
```

**Why?** Old containers can:
- Block ports (8080, 7687)
- Serve stale code (making debug output mysteriously disappear)
- Cause confusing behavior where code changes don't seem to work

### 2. Verify Ports Are Free
```powershell
# Check if ports are available
netstat -ano | Select-String "8080|7687"

# If something is using them, find the process
Get-Process -Id <PID>  # Use PID from netstat output

# Kill if needed
Stop-Process -Id <PID> -Force
```

### 3. Start Fresh ClickHouse (if needed)
```powershell
# Start ClickHouse from docker-compose
docker-compose up -d

# Verify it's running
docker ps | Select-String clickhouse
```

### 4. Clean Build
```powershell
# If you've had mysterious issues, do a clean build
cargo clean
cargo build
```

### 5. Set Environment Variables
```powershell
# Required for server to work
$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"
$env:CLICKHOUSE_DATABASE = "brahmand"
$env:GRAPH_CONFIG_PATH = "social_network.yaml"
$env:RUST_LOG = "debug"  # or "trace" for verbose logging

# Verify they're set
Get-ChildItem Env: | Where-Object { $_.Name -like "CLICKHOUSE*" -or $_.Name -like "GRAPH*" -or $_.Name -eq "RUST_LOG" }
```

### 6. Start Server Correctly
```batch
# RECOMMENDED: Use the batch file (starts in new window)
.\start_server_new_window.bat
```

**OR** in PowerShell (same window):
```powershell
.\start_server_with_env.ps1
```

**NEVER use `Start-Process`** - it doesn't inherit environment variables!

### 7. Verify Server Started
Look for these in server output:
```
✅ Successfully bound HTTP listener to 0.0.0.0:8080
✅ Found GRAPH_CONFIG_PATH: social_network.yaml
✅ Successfully loaded schema from YAML config
  - Loaded 1 node types: ["User"]
```

### 8. Quick Smoke Test
```powershell
# Test simple query
python test_query_simple.py

# Should return:
# Status Code: 200
# ✅ SUCCESS!
# [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}]
```

---

## 🔥 Common Issues & Solutions

### "Address already in use" Error
**Cause**: Old container or process using port 8080  
**Solution**: Run step #1 (Clean Docker Environment)

### "No GRAPH_CONFIG_PATH environment variable found"
**Cause**: Environment variables not set or not inherited  
**Solution**: Use `start_server_with_env.ps1` or `start_server_new_window.bat`

### Debug Output Not Appearing
**Possible Causes**:
1. Old Docker container is responding instead of your build
   - **Solution**: Stop old container (step #1)
2. `RUST_LOG` not set
   - **Solution**: Check environment variables (step #5)
3. Running old binary
   - **Solution**: Do clean build (step #4)

### Code Changes Not Taking Effect
**Possible Causes**:
1. Didn't rebuild after changes
   - **Solution**: `cargo build`
2. Old process still running
   - **Solution**: `Get-Process | Where-Object { $_.Name -like "*brahmand*" } | Stop-Process -Force`
3. **Old Docker container still responding!**
   - **Solution**: `docker stop brahmand; docker rm brahmand`

### Query Returns 500 Error
**Check**:
1. Is ClickHouse running? `docker ps | Select-String clickhouse`
2. Is the OLD container responding? `docker ps -a | Select-String brahmand`
3. Look at server terminal for actual error message

---

## 📝 Quick Reference Commands

```powershell
# Kill all brahmand processes
Get-Process | Where-Object { $_.Name -like "*brahmand*" } | Stop-Process -Force

# Check what's using port 8080
netstat -ano | Select-String "8080"

# Clean Docker completely
docker stop brahmand; docker rm brahmand

# Clean build
cargo clean; cargo build

# Check if server binary is recent
Get-Item .\target\debug\brahmand.exe | Select-Object Name, LastWriteTime

# View server logs
# (Server must be running in separate window or background)

# Test query
python test_query_simple.py
```

---

## 🎯 Session Start Routine (Copy-Paste)

```powershell
# 1. Clean Docker
docker stop brahmand 2>$null; docker rm brahmand 2>$null

# 2. Verify ports free
netstat -ano | Select-String "8080|7687"

# 3. Start ClickHouse if needed
docker-compose up -d

# 4. Start server in new window
.\start_server_new_window.bat

# 5. Wait 3 seconds for server to start
Start-Sleep -Seconds 3

# 6. Test query
python test_query_simple.py
```

If all green, you're ready to code! 🚀
