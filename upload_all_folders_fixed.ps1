# PowerShell скрипт для копирования всех папок проекта на удаленный сервер
# Использование: .\upload_all_folders_fixed.ps1

# Устанавливаем кодировку для корректного отображения русского текста
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$RemoteUser = "dc-srv"
$RemoteHost = "office-srv"
$RemotePath = "~/parser_dist"

Write-Host "Начинаю копирование всех папок на $RemoteUser@$RemoteHost`:$RemotePath" -ForegroundColor Green

# Создаем основные папки на удаленном сервере
Write-Host "Создаю структуру папок на удаленном сервере..." -ForegroundColor Yellow
ssh "$RemoteUser@$RemoteHost" "mkdir -p $RemotePath/{A_TREAD,core,logs,marvel,merlion,netlab,ocs,resursmedio,static,treolan,vvp}"

# Копируем папку A_TREAD
Write-Host "Копирую папку A_TREAD..." -ForegroundColor Cyan
scp -r A_TREAD/* "$RemoteUser@$RemoteHost`:$RemotePath/A_TREAD/"

# Копируем папку core
Write-Host "Копирую папку core..." -ForegroundColor Cyan
scp -r core/* "$RemoteUser@$RemoteHost`:$RemotePath/core/"

# Копируем папку logs
Write-Host "Копирую папку logs..." -ForegroundColor Cyan
scp -r logs/* "$RemoteUser@$RemoteHost`:$RemotePath/logs/"

# Копируем папку marvel
Write-Host "Копирую папку marvel..." -ForegroundColor Cyan
scp -r marvel/* "$RemoteUser@$RemoteHost`:$RemotePath/marvel/"

# Копируем папку merlion
Write-Host "Копирую папку merlion..." -ForegroundColor Cyan
scp -r merlion/* "$RemoteUser@$RemoteHost`:$RemotePath/merlion/"

# Копируем папку netlab
Write-Host "Копирую папку netlab..." -ForegroundColor Cyan
scp -r netlab/* "$RemoteUser@$RemoteHost`:$RemotePath/netlab/"

# Копируем папку ocs
Write-Host "Копирую папку ocs..." -ForegroundColor Cyan
scp -r ocs/* "$RemoteUser@$RemoteHost`:$RemotePath/ocs/"

# Копируем папку resursmedio
Write-Host "Копирую папку resursmedio..." -ForegroundColor Cyan
scp -r resursmedio/* "$RemoteUser@$RemoteHost`:$RemotePath/resursmedio/"

# Копируем папку static
Write-Host "Копирую папку static..." -ForegroundColor Cyan
scp -r static/* "$RemoteUser@$RemoteHost`:$RemotePath/static/"

# Копируем папку treolan
Write-Host "Копирую папку treolan..." -ForegroundColor Cyan
scp -r treolan/* "$RemoteUser@$RemoteHost`:$RemotePath/treolan/"

# Копируем папку vvp
Write-Host "Копирую папку vvp..." -ForegroundColor Cyan
scp -r vvp/* "$RemoteUser@$RemoteHost`:$RemotePath/vvp/"

# Проверяем и копируем папку tests только если она существует и не пуста
if (Test-Path "tests" -PathType Container) {
    $testFiles = Get-ChildItem "tests" -File
    if ($testFiles.Count -gt 0) {
        Write-Host "Копирую папку tests..." -ForegroundColor Cyan
        ssh "$RemoteUser@$RemoteHost" "mkdir -p $RemotePath/tests"
        scp -r tests/* "$RemoteUser@$RemoteHost`:$RemotePath/tests/"
    } else {
        Write-Host "Папка tests пуста, пропускаю..." -ForegroundColor Yellow
    }
} else {
    Write-Host "Папка tests не существует, пропускаю..." -ForegroundColor Yellow
}

# Копируем основные файлы проекта
Write-Host "Копирую основные файлы проекта..." -ForegroundColor Yellow
$mainFiles = @("main.py", "config.py", "production_config.py", "api.py", "server.py", "deploy.sh", "deploy_ubuntu.sh", "update_server.sh", "ocs_categories_cache.json")

foreach ($file in $mainFiles) {
    if (Test-Path $file) {
        Write-Host "Копирую $file..." -ForegroundColor White
        scp $file "$RemoteUser@$RemoteHost`:$RemotePath/"
    } else {
        Write-Host "Файл $file не найден, пропускаю..." -ForegroundColor Yellow
    }
}

# Копируем тестовые файлы
Write-Host "Копирую тестовые файлы..." -ForegroundColor Yellow
$testPatterns = @("test_*.py", "check_*.py", "show_*.py", "compare_*.py", "activate_*.py")

foreach ($pattern in $testPatterns) {
    $files = Get-ChildItem -Name $pattern -ErrorAction SilentlyContinue
    if ($files) {
        foreach ($file in $files) {
            Write-Host "Копирую $file..." -ForegroundColor White
            scp $file "$RemoteUser@$RemoteHost`:$RemotePath/"
        }
    }
}

# Копируем send_db_stats.py отдельно
if (Test-Path "send_db_stats.py") {
    Write-Host "Копирую send_db_stats.py..." -ForegroundColor White
    scp send_db_stats.py "$RemoteUser@$RemoteHost`:$RemotePath/"
}

Write-Host "Копирование завершено!" -ForegroundColor Green
Write-Host "Все папки и файлы скопированы на $RemoteUser@$RemoteHost`:$RemotePath" -ForegroundColor Green 