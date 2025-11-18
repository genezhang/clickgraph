# Stop ClickGraph Wiki Validation Server
# Run this to stop the background server started by setup_wiki_validation.ps1

Write-Host "`n🛑 Stopping ClickGraph validation server...`n" -ForegroundColor Yellow

# Try to read saved job ID
if (Test-Path ".clickgraph_job_id") {
    $jobId = Get-Content ".clickgraph_job_id"
    
    $job = Get-Job -Id $jobId -ErrorAction SilentlyContinue
    
    if ($job) {
        Write-Host "  Found server job (ID: $jobId)" -ForegroundColor Green
        
        # Show last few lines of output
        Write-Host "`n  Last output from server:" -ForegroundColor Cyan
        Receive-Job -Id $jobId -Keep | Select-Object -Last 10
        
        # Stop and remove job
        Stop-Job -Id $jobId
        Remove-Job -Id $jobId
        
        Write-Host "`n  ✅ Server stopped" -ForegroundColor Green
        
        # Clean up job ID file
        Remove-Item ".clickgraph_job_id" -Force
    } else {
        Write-Host "  ⚠️  No job found with ID: $jobId" -ForegroundColor Yellow
        Write-Host "     It may have already stopped" -ForegroundColor Gray
    }
} else {
    Write-Host "  ℹ️  No saved job ID found" -ForegroundColor Cyan
    Write-Host "     Checking for any ClickGraph jobs..." -ForegroundColor Gray
    
    # Try to find any jobs
    $jobs = Get-Job | Where-Object { $_.Command -like "*clickgraph*" }
    
    if ($jobs) {
        Write-Host "     Found $($jobs.Count) ClickGraph job(s):" -ForegroundColor Yellow
        foreach ($job in $jobs) {
            Write-Host "       Job ID: $($job.Id), State: $($job.State)" -ForegroundColor Gray
            Stop-Job -Id $job.Id
            Remove-Job -Id $job.Id
            Write-Host "       ✅ Stopped job $($job.Id)" -ForegroundColor Green
        }
    } else {
        Write-Host "     No ClickGraph jobs found" -ForegroundColor Gray
    }
}

Write-Host "`n✅ Cleanup complete`n" -ForegroundColor Green
