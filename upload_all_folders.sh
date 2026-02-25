#!/bin/bash

# Скрипт для копирования всех папок проекта на удаленный сервер
# Использование: ./upload_all_folders.sh

REMOTE_USER="dc-srv"
REMOTE_HOST="office-srv"
REMOTE_PATH="~/parser_dist"

echo "Начинаю копирование всех папок на $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH"

# Создаем основные папки на удаленном сервере
echo "Создаю структуру папок на удаленном сервере..."
ssh $REMOTE_USER@$REMOTE_HOST "mkdir -p $REMOTE_PATH/{A_TREAD,core,logs,marvel,merlion,netlab,ocs,resursmedio,static,treolan,vvp,tests}"

# Копируем папку A_TREAD
echo "Копирую папку A_TREAD..."
scp -r A_TREAD/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/A_TREAD/

# Копируем папку core
echo "Копирую папку core..."
scp -r core/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/core/

# Копируем папку logs
echo "Копирую папку logs..."
scp -r logs/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/logs/

# Копируем папку marvel
echo "Копирую папку marvel..."
scp -r marvel/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/marvel/

# Копируем папку merlion
echo "Копирую папку merlion..."
scp -r merlion/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/merlion/

# Копируем папку netlab
echo "Копирую папку netlab..."
scp -r netlab/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/netlab/

# Копируем папку ocs
echo "Копирую папку ocs..."
scp -r ocs/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/ocs/

# Копируем папку resursmedio
echo "Копирую папку resursmedio..."
scp -r resursmedio/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/resursmedio/

# Копируем папку static
echo "Копирую папку static..."
scp -r static/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/static/

# Копируем папку treolan
echo "Копирую папку treolan..."
scp -r treolan/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/treolan/

# Копируем папку vvp
echo "Копирую папку vvp..."
scp -r vvp/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/vvp/

# Копируем папку tests
echo "Копирую папку tests..."
scp -r tests/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/tests/

# Копируем основные файлы проекта
echo "Копирую основные файлы проекта..."
scp main.py $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp config.py $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp production_config.py $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp api.py $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp server.py $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp deploy.sh $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp deploy_ubuntu.sh $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp update_server.sh $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp ocs_categories_cache.json $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/

# Копируем тестовые файлы
echo "Копирую тестовые файлы..."
scp test_*.py $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp check_*.py $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp show_*.py $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp compare_*.py $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp activate_*.py $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/
scp send_db_stats.py $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/

echo "Копирование завершено!"
echo "Все папки и файлы скопированы на $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH" 