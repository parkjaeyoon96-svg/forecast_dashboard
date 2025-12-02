# C:\ke30 폴더 모니터링 스크립트
# 새 Excel 파일이 생성되면 자동으로 당년 데이터 처리 배치 실행

$folder = "C:\ke30"
$batFile = Join-Path $PSScriptRoot "당년데이터_처리실행.bat"

# 배치 파일 존재 확인
if (-not (Test-Path $batFile)) {
    Write-Host "오류: 배치 파일을 찾을 수 없습니다: $batFile" -ForegroundColor Red
    exit 1
}

# 폴더 존재 확인
if (-not (Test-Path $folder)) {
    Write-Host "오류: 폴더를 찾을 수 없습니다: $folder" -ForegroundColor Red
    exit 1
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  KE30 폴더 모니터링 시작" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "모니터링 폴더: $folder" -ForegroundColor Yellow
Write-Host "배치 파일: $batFile" -ForegroundColor Yellow
Write-Host ""
Write-Host "새 Excel 파일이 생성되면 자동으로 처리됩니다." -ForegroundColor Green
Write-Host "종료하려면 Ctrl+C를 누르세요." -ForegroundColor Gray
Write-Host ""

$watcher = New-Object System.IO.FileSystemWatcher
$watcher.Path = $folder
$watcher.Filter = "*.xlsx"
$watcher.NotifyFilter = [System.IO.NotifyFilters]::FileName -bor [System.IO.NotifyFilters]::LastWrite
$watcher.EnableRaisingEvents = $true

$action = {
    $details = $event.SourceEventArgs
    $name = $details.Name
    $changeType = $details.ChangeType
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    
    if ($changeType -eq "Created" -or $changeType -eq "Changed") {
        Write-Host "[$timestamp] 새 파일 감지: $name" -ForegroundColor Green
        Write-Host "배치 파일 실행 중..." -ForegroundColor Yellow
        
        $batDir = Split-Path $batFile -Parent
        Start-Process -FilePath $batFile -WorkingDirectory $batDir -Wait -NoNewWindow
        
        Write-Host "[$timestamp] 처리 완료: $name" -ForegroundColor Cyan
        Write-Host ""
    }
}

Register-ObjectEvent $watcher "Created" -Action $action | Out-Null
Register-ObjectEvent $watcher "Changed" -Action $action | Out-Null

try {
    while ($true) {
        Start-Sleep -Seconds 1
    }
} finally {
    $watcher.Dispose()
    Write-Host "모니터링 종료" -ForegroundColor Yellow
}

