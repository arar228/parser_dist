#!/bin/bash

# Упрощенный скрипт для копирования всех папок проекта
# Использование: ./upload_folders_simple.sh

REMOTE_USER="dc-srv"
REMOTE_HOST="office-srv"
REMOTE_PATH="~/parser_dist"

echo "Копирую все папки на $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH"

# Создаем структуру папок и копируем содержимое
for folder in A_TREAD core logs marvel merlion netlab ocs resursmedio static treolan vvp tests; do
    echo "Обрабатываю папку: $folder"
    ssh $REMOTE_USER@$REMOTE_HOST "mkdir -p $REMOTE_PATH/$folder"
    scp -r $folder/* $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/$folder/ 2>/dev/null || echo "Папка $folder пуста или не существует"
done

# Копируем основные файлы
echo "Копирую основные файлы..."
scp *.py *.sh *.json *.txt $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/ 2>/dev/null || echo "Некоторые файлы не найдены"

echo "Копирование завершено!" 