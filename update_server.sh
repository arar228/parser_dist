#!/bin/bash

# Скрипт для обновления только измененных файлов на сервере
# Использование: ./update_server.sh

SERVER_IP="192.168.10.151"
SERVER_PORT="5901"
SERVER_USER="root"  # Измените на нужного пользователя
SERVER_PATH="/opt/parser_dist"  # Путь к проекту на сервере

echo "Обновление файлов на сервере $SERVER_IP:$SERVER_PORT..."

# Создаем временную директорию для файлов
TEMP_DIR="./temp_update"
mkdir -p $TEMP_DIR

# Копируем измененные файлы во временную директорию
echo "Подготовка файлов для обновления..."

# Создаем структуру директорий
mkdir -p $TEMP_DIR/static

# Копируем измененные файлы
cp static/index.html $TEMP_DIR/static/
cp api.py $TEMP_DIR/

echo "Файлы подготовлены в директории $TEMP_DIR"

# Копируем файлы на сервер
echo "Копирование файлов на сервер..."

# Вариант 1: Если есть SSH доступ
if command -v scp &> /dev/null; then
    echo "Используем SCP для копирования..."
    scp -P $SERVER_PORT -r $TEMP_DIR/* $SERVER_USER@$SERVER_IP:$SERVER_PATH/
    
    if [ $? -eq 0 ]; then
        echo "✅ Файлы успешно скопированы на сервер"
    else
        echo "❌ Ошибка при копировании файлов"
        exit 1
    fi
else
    echo "SCP не найден. Используйте альтернативные методы:"
    echo "1. Скопируйте содержимое $TEMP_DIR в $SERVER_PATH на сервере"
    echo "2. Или используйте rsync: rsync -avz -e 'ssh -p $SERVER_PORT' $TEMP_DIR/ $SERVER_USER@$SERVER_IP:$SERVER_PATH/"
fi

# Очистка временной директории
rm -rf $TEMP_DIR

echo "Обновление завершено!"
echo "Не забудьте перезапустить сервисы на сервере:"
echo "sudo systemctl restart parser-dist.service parser-web.service" 