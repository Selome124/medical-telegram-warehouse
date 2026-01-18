Write-Host " TROUBLESHOOTING TASK 1" -ForegroundColor Cyan
Write-Host "=" * 50 -ForegroundColor Cyan
Write-Host ""

Write-Host "PROBLEM: Password prompt / 2FA issues" -ForegroundColor Yellow
Write-Host ""
Write-Host "SOLUTION 1: Enter your Telegram 2FA password" -ForegroundColor White
Write-Host "When asked: 'Please enter your password:'" -ForegroundColor Gray
Write-Host "Enter the password you use to login to Telegram app" -ForegroundColor Gray
Write-Host "(This is NOT your phone/SMS code)" -ForegroundColor Gray
Write-Host ""

Write-Host "SOLUTION 2: If you forgot 2FA password" -ForegroundColor White
Write-Host "1. Open Telegram app on your phone" -ForegroundColor Gray
Write-Host "2. Go to Settings > Privacy and Security" -ForegroundColor Gray
Write-Host "3. Click 'Two-Step Verification'" -ForegroundColor Gray
Write-Host "4. You can change or disable it there" -ForegroundColor Gray
Write-Host ""

Write-Host "SOLUTION 3: Use a different account (without 2FA)" -ForegroundColor White
Write-Host "1. Create a new Telegram account" -ForegroundColor Gray
Write-Host "2. Don't enable 2FA on it" -ForegroundColor Gray
Write-Host "3. Get new API credentials for that account" -ForegroundColor Gray
Write-Host "4. Update .env with new credentials" -ForegroundColor Gray
Write-Host ""

Write-Host "SOLUTION 4: Try with a bot token instead" -ForegroundColor White
Write-Host "1. Talk to @BotFather on Telegram" -ForegroundColor Gray
Write-Host "2. Create a new bot" -ForegroundColor Gray
Write-Host "3. Get bot token" -ForegroundColor Gray
Write-Host "4. Use token instead of phone number" -ForegroundColor Gray
Write-Host ""

Write-Host "TRY THIS FIRST:" -ForegroundColor Green
Write-Host "1. Run the test connection:" -ForegroundColor White
Write-Host "   python src/test_connection.py" -ForegroundColor Gray
Write-Host ""
Write-Host "2. When asked for password, enter your Telegram 2FA password" -ForegroundColor White
Write-Host "   (or press Enter if you don't have 2FA)" -ForegroundColor Gray
Write-Host ""
Write-Host "3. If successful, run the full scraper:" -ForegroundColor White
Write-Host "   python src/task1_final.py" -ForegroundColor Gray
