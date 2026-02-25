# PowerShell script for copying all project folders to remote server
# Usage: .\upload_simple_en.ps1

$RemoteUser = "dc-srv"
$RemoteHost = "office-srv"
$RemotePath = "~/parser_dist"

Write-Host "Starting to copy all folders to $RemoteUser@$RemoteHost`:$RemotePath" -ForegroundColor Green

# Create main folders on remote server
Write-Host "Creating folder structure on remote server..." -ForegroundColor Yellow
ssh "$RemoteUser@$RemoteHost" "mkdir -p $RemotePath/{A_TREAD,core,logs,marvel,merlion,netlab,ocs,resursmedio,static,treolan,vvp}"

# Copy A_TREAD folder
Write-Host "Copying A_TREAD folder..." -ForegroundColor Cyan
scp -r A_TREAD/* "$RemoteUser@$RemoteHost`:$RemotePath/A_TREAD/"

# Copy core folder
Write-Host "Copying core folder..." -ForegroundColor Cyan
scp -r core/* "$RemoteUser@$RemoteHost`:$RemotePath/core/"

# Copy logs folder
Write-Host "Copying logs folder..." -ForegroundColor Cyan
scp -r logs/* "$RemoteUser@$RemoteHost`:$RemotePath/logs/"

# Copy marvel folder
Write-Host "Copying marvel folder..." -ForegroundColor Cyan
scp -r marvel/* "$RemoteUser@$RemoteHost`:$RemotePath/marvel/"

# Copy merlion folder
Write-Host "Copying merlion folder..." -ForegroundColor Cyan
scp -r merlion/* "$RemoteUser@$RemoteHost`:$RemotePath/merlion/"

# Copy netlab folder
Write-Host "Copying netlab folder..." -ForegroundColor Cyan
scp -r netlab/* "$RemoteUser@$RemoteHost`:$RemotePath/netlab/"

# Copy ocs folder
Write-Host "Copying ocs folder..." -ForegroundColor Cyan
scp -r ocs/* "$RemoteUser@$RemoteHost`:$RemotePath/ocs/"

# Copy resursmedio folder
Write-Host "Copying resursmedio folder..." -ForegroundColor Cyan
scp -r resursmedio/* "$RemoteUser@$RemoteHost`:$RemotePath/resursmedio/"

# Copy static folder
Write-Host "Copying static folder..." -ForegroundColor Cyan
scp -r static/* "$RemoteUser@$RemoteHost`:$RemotePath/static/"

# Copy treolan folder
Write-Host "Copying treolan folder..." -ForegroundColor Cyan
scp -r treolan/* "$RemoteUser@$RemoteHost`:$RemotePath/treolan/"

# Copy vvp folder
Write-Host "Copying vvp folder..." -ForegroundColor Cyan
scp -r vvp/* "$RemoteUser@$RemoteHost`:$RemotePath/vvp/"

# Check and copy tests folder if it exists and not empty
if (Test-Path "tests" -PathType Container) {
    $testFiles = Get-ChildItem "tests" -File
    if ($testFiles.Count -gt 0) {
        Write-Host "Copying tests folder..." -ForegroundColor Cyan
        ssh "$RemoteUser@$RemoteHost" "mkdir -p $RemotePath/tests"
        scp -r tests/* "$RemoteUser@$RemoteHost`:$RemotePath/tests/"
    } else {
        Write-Host "Tests folder is empty, skipping..." -ForegroundColor Yellow
    }
} else {
    Write-Host "Tests folder does not exist, skipping..." -ForegroundColor Yellow
}

# Copy main project files
Write-Host "Copying main project files..." -ForegroundColor Yellow
$mainFiles = @("main.py", "config.py", "production_config.py", "api.py", "server.py", "deploy.sh", "deploy_ubuntu.sh", "update_server.sh", "ocs_categories_cache.json")

foreach ($file in $mainFiles) {
    if (Test-Path $file) {
        Write-Host "Copying $file..." -ForegroundColor White
        scp $file "$RemoteUser@$RemoteHost`:$RemotePath/"
    } else {
        Write-Host "File $file not found, skipping..." -ForegroundColor Yellow
    }
}

# Copy test files
Write-Host "Copying test files..." -ForegroundColor Yellow
$testPatterns = @("test_*.py", "check_*.py", "show_*.py", "compare_*.py", "activate_*.py")

foreach ($pattern in $testPatterns) {
    $files = Get-ChildItem -Name $pattern -ErrorAction SilentlyContinue
    if ($files) {
        foreach ($file in $files) {
            Write-Host "Copying $file..." -ForegroundColor White
            scp $file "$RemoteUser@$RemoteHost`:$RemotePath/"
        }
    }
}

# Copy send_db_stats.py separately
if (Test-Path "send_db_stats.py") {
    Write-Host "Copying send_db_stats.py..." -ForegroundColor White
    scp send_db_stats.py "$RemoteUser@$RemoteHost`:$RemotePath/"
}

Write-Host "Copying completed!" -ForegroundColor Green
Write-Host "All folders and files copied to $RemoteUser@$RemoteHost`:$RemotePath" -ForegroundColor Green 