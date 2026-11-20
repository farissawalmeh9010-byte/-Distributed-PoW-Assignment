
$ErrorActionPreference = "Stop"

# Always work from the project root.
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

New-Item -ItemType Directory -Force "build", "report", "report\scalability-logs" | Out-Null

Write-Host "===== BUILDING GO APPLICATIONS ====="

go build -o "build\coordinator.exe" ./cmd/coordinator
if ($LASTEXITCODE -ne 0) { throw "Coordinator build failed" }

go build -o "build\worker.exe" ./cmd/worker
if ($LASTEXITCODE -ne 0) { throw "Worker build failed" }

$coordinatorExe = Join-Path $projectRoot "build\coordinator.exe"
$workerExe = Join-Path $projectRoot "build\worker.exe"
$logDir = Join-Path $projectRoot "report\scalability-logs"

$results = @()
$workerCounts = @(1, 2, 4, 8)

foreach ($count in $workerCounts) {
    Write-Host "`n===== TEST WITH $count WORKERS ====="

    $processes = @()

    try {
        # Start a fresh coordinator.
        $coord = Start-Process `
            -FilePath $coordinatorExe `
            -ArgumentList @(
                "-grpc-port", "50051",
                "-http-port", "8080",
                "-range-size", "1000000"
            ) `
            -RedirectStandardOutput "$logDir\coordinator-$count.out.log" `
            -RedirectStandardError "$logDir\coordinator-$count.err.log" `
            -PassThru -WindowStyle Hidden

        $processes += $coord

        # Wait for the HTTP server.
        $ready = $false
        for ($attempt = 0; $attempt -lt 50; $attempt++) {
            Start-Sleep -Milliseconds 200

            if ($coord.HasExited) {
                throw "Coordinator exited unexpectedly"
            }

            try {
                $null = Invoke-RestMethod "http://localhost:8080/workers"
                $ready = $true
                break
            }
            catch {}
        }

        if (-not $ready) {
            throw "Coordinator did not become ready"
        }

        # Start the required number of workers.
        for ($i = 1; $i -le $count; $i++) {
            $port = 50100 + $i
            $workerId = "benchmark-$count-$i"

            $proc = Start-Process `
                -FilePath $workerExe `
                -ArgumentList @(
                    "-id", $workerId,
                    "-port", "$port",
                    "-coordinator", "localhost:50051"
                ) `
                -RedirectStandardOutput "$logDir\worker-$count-$i.out.log" `
                -RedirectStandardError "$logDir\worker-$count-$i.err.log" `
                -PassThru -WindowStyle Hidden

            $processes += $proc
        }

        # Ensure all workers registered before measuring.
        $registered = $false

        for ($attempt = 0; $attempt -lt 75; $attempt++) {
            Start-Sleep -Milliseconds 200

            $workers = @(Invoke-RestMethod "http://localhost:8080/workers" | ForEach-Object { $_ })
            $alive = @($workers | Where-Object { $_.alive -eq $true })

            if ($alive.Count -eq $count) {
                $registered = $true
                break
            }
        }

        if (-not $registered) {
            throw "Expected $count workers, got $($alive.Count)"
        }

        Write-Host "Registered workers: $($alive.Count)"

        # Submit the same PoW problem for every run.
        $body = @{
            data = "scalability-test"
            difficulty = 5
        } | ConvertTo-Json

        $job = Invoke-RestMethod `
            -Uri "http://localhost:8080/jobs" `
            -Method Post `
            -ContentType "application/json" `
            -Body $body

        $deadline = (Get-Date).AddMinutes(3)

        do {
            Start-Sleep -Milliseconds 100

            $status = Invoke-RestMethod `
                -Uri "http://localhost:8080/jobs/$($job.job_id)"

            if ((Get-Date) -gt $deadline) {
                throw "Benchmark timed out"
            }

        } while ($status.status -eq "RUNNING")

        if ($status.status -ne "FOUND") {
            throw "Job ended with status $($status.status)"
        }

        # Independently verify the reported hash.
        $inputText = "scalability-test" + $status.nonce
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($inputText)
        $sha = [System.Security.Cryptography.SHA256]::Create()

        try {
            $hashBytes = $sha.ComputeHash($bytes)
            $calculatedHash = [System.BitConverter]::ToString(
                $hashBytes
            ).Replace("-", "").ToLowerInvariant()
        }
        finally {
            $sha.Dispose()
        }

        if ($calculatedHash -ne $status.hash -or
            -not $calculatedHash.StartsWith("00000")) {
            throw "PoW verification failed"
        }

        $results += [PSCustomObject]@{
            Workers = $count
            Difficulty = 5
            Status = $status.status
            Nonce = $status.nonce
            Checked = $status.checked
            TimeSeconds = [math]::Round(
                [double]$status.elapsed_seconds, 4
            )
        }

        Write-Host "Status: $($status.status)"
        Write-Host "Nonce: $($status.nonce)"
        Write-Host "Time: $($status.elapsed_seconds) seconds"
        Write-Host "Hash verified successfully"
    }
    finally {
        # Stop all processes started for this experiment.
        foreach ($proc in $processes) {
            if ($null -ne $proc) {
                Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            }
        }

        Start-Sleep -Seconds 2
    }
}

# Calculate speedup relative to one worker.
$baseline = ($results | Where-Object {
    $_.Workers -eq 1
}).TimeSeconds

$final = foreach ($r in $results) {
    [PSCustomObject]@{
        Workers = $r.Workers
        Difficulty = $r.Difficulty
        TimeSeconds = $r.TimeSeconds
        Speedup = [math]::Round(
            $baseline / $r.TimeSeconds, 3
        )
        Nonce = $r.Nonce
        Checked = $r.Checked
        Status = $r.Status
    }
}

Write-Host "`n===== SCALABILITY RESULTS ====="
$final | Format-Table -AutoSize

$final | Export-Csv `
    -Path "report\scalability_results.csv" `
    -NoTypeInformation `
    -Encoding UTF8

Write-Host "`nResults saved to report\scalability_results.csv"
