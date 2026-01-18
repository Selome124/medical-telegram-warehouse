Write-Host " GETTING TELEGRAM API CREDENTIALS - VISUAL GUIDE" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Cyan
Write-Host ""

Write-Host "STEP 1: Open browser and go to:" -ForegroundColor Yellow
Write-Host "        https://my.telegram.org" -ForegroundColor White
Write-Host ""
Write-Host "        You'll see:" -ForegroundColor Gray
Write-Host "        " -ForegroundColor DarkGray
Write-Host "         Welcome to Telegram         " -ForegroundColor DarkGray
Write-Host "        " -ForegroundColor DarkGray
Write-Host "         Country: Ethiopia          " -ForegroundColor DarkGray
Write-Host "         Phone: +251 ___________     " -ForegroundColor DarkGray
Write-Host "                                     " -ForegroundColor DarkGray
Write-Host "                 [ Next ]            " -ForegroundColor DarkGray
Write-Host "        " -ForegroundColor DarkGray

Write-Host "`nSTEP 2: Enter your Ethiopian phone number:" -ForegroundColor Yellow
Write-Host "        Format: +2519XXXXXXXX" -ForegroundColor White
Write-Host "        Example: +251911223344" -ForegroundColor Gray
Write-Host "        Click NEXT" -ForegroundColor Gray

Write-Host "`nSTEP 3: Check Telegram app for code:" -ForegroundColor Yellow
Write-Host "        Open Telegram on your phone" -ForegroundColor White
Write-Host "        You'll get a login code like: 12345" -ForegroundColor Gray
Write-Host "        Enter code on website" -ForegroundColor Gray

Write-Host "`nSTEP 4: Click 'API Development Tools':" -ForegroundColor Yellow
Write-Host "        After login, click this button" -ForegroundColor White
Write-Host "        " -ForegroundColor DarkGray
Write-Host "         [] API Development Tools   " -ForegroundColor DarkGray
Write-Host "         [ ] Other options           " -ForegroundColor DarkGray
Write-Host "        " -ForegroundColor DarkGray

Write-Host "`nSTEP 5: Fill the form EXACTLY:" -ForegroundColor Yellow
Write-Host "        " -ForegroundColor DarkGray
Write-Host "         App title: Medical Telegram " -ForegroundColor DarkGray
Write-Host "                   Scraper           " -ForegroundColor DarkGray
Write-Host "        " -ForegroundColor DarkGray
Write-Host "         Short name: medscraper      " -ForegroundColor DarkGray
Write-Host "        " -ForegroundColor DarkGray
Write-Host "         Platform: Web              " -ForegroundColor DarkGray
Write-Host "        " -ForegroundColor DarkGray
Write-Host "         Description: Data scraping  " -ForegroundColor DarkGray
Write-Host "         for medical channels        " -ForegroundColor DarkGray
Write-Host "        " -ForegroundColor DarkGray
Write-Host "        Click: [ Create application ]" -ForegroundColor White

Write-Host "`nSTEP 6: COPY your credentials:" -ForegroundColor Yellow
Write-Host "        You'll see:" -ForegroundColor White
Write-Host "        " -ForegroundColor DarkGray
Write-Host "         api_id: 28463712            " -ForegroundColor DarkGray
Write-Host "                                     " -ForegroundColor DarkGray
Write-Host "         api_hash: a1b2c3d4e5...     " -ForegroundColor DarkGray
Write-Host "        " -ForegroundColor DarkGray
Write-Host "        COPY BOTH! Write them down." -ForegroundColor Red

Write-Host "`nSTEP 7: Come back here and run:" -ForegroundColor Yellow
Write-Host "        .\setup_credentials.ps1" -ForegroundColor White
Write-Host "        Then enter your api_id and api_hash" -ForegroundColor Gray

Write-Host "`n" + "=" * 60 -ForegroundColor Cyan
Write-Host " TIP: If stuck, take a screenshot and show me!" -ForegroundColor Yellow
Write-Host "=" * 60 -ForegroundColor Cyan
