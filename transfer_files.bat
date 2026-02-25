@echo off
echo Переносим исправленные файлы на сервер...
echo.

echo 1. Основные файлы:
scp -P 22 main.py dc-srv@192.168.10.151:~/parser_dist/
scp -P 22 mysql_config.py dc-srv@192.168.10.151:~/parser_dist/
scp -P 22 production_config.py dc-srv@192.168.10.151:~/parser_dist/

echo.
echo 2. Модули дистрибьюторов:
scp -P 22 merlion/merlion_export.py dc-srv@192.168.10.151:~/parser_dist/merlion/
scp -P 22 merlion/merlion_downloader_fixed.py dc-srv@192.168.10.151:~/parser_dist/merlion/
scp -P 22 marvel/marvel_export.py dc-srv@192.168.10.151:~/parser_dist/marvel/

echo.
echo 3. Core модули:
scp -P 22 core/telegram_notify.py dc-srv@192.168.10.151:~/parser_dist/core/

echo.
echo Все файлы перенесены на сервер!
echo Теперь можно запускать выгрузку.
pause
