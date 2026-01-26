# --- Settings ---
$Portal = "vpn2.tawa.com"
$TargetSQL = "tawasql5"
$GPPath = "C:\Program Files\Palo Alto Networks\GlobalProtect"

Write-Host "--- Starting GlobalProtect Auto-Connect (Smart Version) ---" -ForegroundColor Cyan
Write-Host "WARNING: DO NOT LOCK SCREEN (Win+L). Please turn off monitor power instead." -ForegroundColor Yellow

# --- [Pre-Check] Verify if connection already exists ---
Write-Host "Checking current connection status..."
$preCheck = Test-NetConnection $TargetSQL -Port 1433 -WarningAction SilentlyContinue
if ($preCheck.TcpTestSucceeded) {
    Write-Host "VPN is already CONNECTED and $TargetSQL is reachable." -ForegroundColor Green
    Write-Host "Skipping connection steps to avoid accidental disconnection." -ForegroundColor Green
    exit 0  # Terminate script early as goal is already met
}

# --- If connection check failed, proceed with connection logic ---
Write-Host "VPN not connected. Starting connection process..." -ForegroundColor Gray

# 1. Change Directory to GlobalProtect folder
Push-Location $GPPath

# 2. Launch PanGPA with connect parameter
Write-Host "Triggering VPN Connect UI..."
Start-Process ".\PanGPA.exe" -ArgumentList "-connect -portal $Portal"

# 3. Wait for UI and Send Keys
Start-Sleep -Seconds 5
$wshell = New-Object -ComObject WScript.Shell

for ($i = 1; $i -le 3; $i++) {
    # Attempt to activate GlobalProtect window
    $success = $wshell.AppActivate("GlobalProtect")
    if ($success) {
        Write-Host "Attempt ${i}: Window found, shifting focus and connecting..."
        Start-Sleep -Milliseconds 800
        # Send TAB to move focus to 'Connect' button, then Enter (~)
        $wshell.SendKeys("{TAB}~") 
        Start-Sleep -Seconds 5 
    } else {
        Write-Host "Attempt ${i}: Window not found, retrying..."
        Start-Sleep -Seconds 2
    }
}

# 4. Wait for Tunnel Establishment (60s recommended for overseas connection)
Write-Host "Waiting 60 seconds for secure tunnel establishment..." -ForegroundColor Yellow
Start-Sleep -Seconds 660

# 5. Final Connection Validation
$check = Test-NetConnection $TargetSQL -Port 1433 -WarningAction SilentlyContinue

if ($check.TcpTestSucceeded) {
    Write-Host "==============================================" -ForegroundColor Green
    Write-Host "SUCCESS: VPN Connected and $TargetSQL is Reachable!" -ForegroundColor Green
    Write-Host "==============================================" -ForegroundColor Green
} else {
    Write-Host "FAILED: $TargetSQL is still NOT reachable." -ForegroundColor Red
}

Pop-Location