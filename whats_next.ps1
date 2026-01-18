Write-Host " WHAT'S NEXT FOR YOUR PROJECT" -ForegroundColor Cyan
Write-Host "=" * 50 -ForegroundColor Cyan
Write-Host ""

# Check what you need to do
Write-Host "Checking your current setup..." -ForegroundColor Yellow

$missing = @()

# Check .env
if (-not (Test-Path ".env")) {
    $missing += "No .env file"
} else {
    $content = Get-Content ".env" -Raw
    if ($content -match "12345678" -or $content -match "abcdef1234567890") {
        $missing += ".env has EXAMPLE values (need REAL credentials)"
    }
}

# Check Python packages
try {
    python -c "import telethon" 2>$null
} catch {
    $missing += "telethon package not installed"
}

if ($missing.Count -eq 0) {
    Write-Host " Everything looks ready!" -ForegroundColor Green
    Write-Host "`n You can run Task 1 now:" -ForegroundColor Yellow
    Write-Host "   python src/final_scraper.py" -ForegroundColor White
    
    Write-Host "`nThis will:" -ForegroundColor Gray
    Write-Host "1. Connect to Telegram" -ForegroundColor Gray
    Write-Host "2. Scrape 3 medical channels" -ForegroundColor Gray
    Write-Host "3. Save data to data/raw/" -ForegroundColor Gray
    Write-Host "4. Complete Task 1 deliverables" -ForegroundColor Gray
    
    $runNow = Read-Host "`nRun Task 1 scraper now? (y/n)"
    if ($runNow -eq 'y') {
        Write-Host "`nStarting Task 1..." -ForegroundColor Green
        python src/final_scraper.py
        
        Write-Host "`n Task 1 Results:" -ForegroundColor Cyan
        if (Test-Path "data/raw/telegram_messages") {
            $files = Get-ChildItem "data/raw/telegram_messages" -Recurse -File
            Write-Host " Created $($files.Count) JSON files" -ForegroundColor Green
        }
        if (Test-Path "data/raw/images") {
            $folders = Get-ChildItem "data/raw/images" -Directory
            Write-Host " Created $($folders.Count) image folders" -ForegroundColor Green
        }
        
        Write-Host "`n TASK 1 COMPLETE!" -ForegroundColor Green
        Write-Host "You can now move to Task 2" -ForegroundColor Yellow
    }
} else {
    Write-Host "`n Missing items:" -ForegroundColor Red
    $missing | ForEach-Object { Write-Host "   $_" -ForegroundColor Red }
    
    Write-Host "`n Fix these first:" -ForegroundColor Yellow
    if ($missing -contains "No .env file" -or $missing -contains ".env has EXAMPLE values") {
        Write-Host "1. Get Telegram credentials from: https://my.telegram.org" -ForegroundColor White
        Write-Host "2. Run: .\setup_credentials.ps1" -ForegroundColor Gray
    }
    if ($missing -contains "telethon package not installed") {
        Write-Host "1. Install: pip install telethon python-dotenv" -ForegroundColor White
    }
}

Write-Host "`n PROJECT TIMELINE:" -ForegroundColor Magenta
Write-Host "==================" -ForegroundColor Magenta
Write-Host "Task 1: Data Scraping (NOW)         " -ForegroundColor Gray
Write-Host "Task 2: Data Modeling with dbt      " -ForegroundColor Gray
Write-Host "Task 3: YOLO Image Detection        " -ForegroundColor Gray
Write-Host "Task 4: FastAPI Analytical API      " -ForegroundColor Gray
Write-Host "Task 5: Dagster Orchestration       " -ForegroundColor Gray

Write-Host "`n Time estimate: 1-2 hours per task" -ForegroundColor Yellow
