# PowerShell скрипт для копирования всех папок проекта на удаленный сервер
# Использование: .\upload_all_folders.ps1

$RemoteUser = "dc-srv"
$RemoteHost = "office-srv"
$RemotePath = "~/parser_dist"

Write-Host "Начинаю копирование всех папок на $RemoteUser@$RemoteHost`:$RemotePath" -ForegroundColor Green

# Создаем основные папки на удаленном сервере
Write-Host "Создаю структуру папок на удаленном сервере..." -ForegroundColor Yellow
ssh "$RemoteUser@$RemoteHost" "mkdir -p $RemotePath/{A_TREAD,core,logs,marvel,merlion,netlab,ocs,resursmedio,static,treolan,vvp,tests}"

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

# Копируем папку tests
Write-Host "Копирую папку tests..." -ForegroundColor Cyan
scp -r tests/* "$RemoteUser@$RemoteHost`:$RemotePath/tests/"

# Копируем основные файлы проекта
Write-Host "Копирую основные файлы проекта..." -ForegroundColor Yellow
scp main.py "$RemoteUser@$RemoteHost`:$RemotePath/"
scp config.py "$RemoteUser@$RemoteHost`:$RemotePath/"
scp production_config.py "$RemoteUser@$RemoteHost`:$RemotePath/"
scp api.py "$RemoteUser@$RemoteHost`:$RemotePath/"
scp server.py "$RemoteUser@$RemoteHost`:$RemotePath/"
scp deploy.sh "$RemoteUser@$RemoteHost`:$RemotePath/"
scp deploy_ubuntu.sh "$RemoteUser@$RemoteHost`:$RemotePath/"
scp update_server.sh "$RemoteUser@$RemoteHost`:$RemotePath/"
scp ocs_categories_cache.json "$RemoteUser@$RemoteHost`:$RemotePath/"

# Копируем тестовые файлы
Write-Host "Копирую тестовые файлы..." -ForegroundColor Yellow
scp test_*.py "$RemoteUser@$RemoteHost`:$RemotePath/"
scp check_*.py "$RemoteUser@$RemoteHost`:$RemotePath/"
scp show_*.py "$RemoteUser@$RemoteHost`:$RemotePath/"
scp compare_*.py "$RemoteUser@$RemoteHost`:$RemotePath/"
scp activate_*.py "$RemoteUser@$RemoteHost`:$RemotePath/"
scp send_db_stats.py "$RemoteUser@$RemoteHost`:$RemotePath/"

Write-Host "Копирование завершено!" -ForegroundColor Green
Write-Host "Все папки и файлы скопированы на $RemoteUser@$RemoteHost`:$RemotePath" -ForegroundColor Green 