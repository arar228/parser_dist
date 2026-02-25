#!/bin/bash

# Скрипт автоматического развертывания для Ubuntu/Debian
# Использование: ./deploy_ubuntu.sh [username]

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функция для вывода сообщений
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
    exit 1
}

# Проверка прав root
if [[ $EUID -eq 0 ]]; then
   error "Этот скрипт не должен запускаться от root. Запустите от обычного пользователя."
fi

# Получение имени пользователя
USERNAME=${1:-$USER}
PROJECT_DIR="/opt/parser_dist"
BACKUP_DIR="/opt/backups/parser_dist"

log "Начинаем развертывание проекта parser_dist для пользователя: $USERNAME"

# Определение дистрибутива
if command -v apt-get &> /dev/null; then
    PACKAGE_MANAGER="apt"
elif command -v dnf &> /dev/null; then
    PACKAGE_MANAGER="dnf"
elif command -v yum &> /dev/null; then
    PACKAGE_MANAGER="yum"
else
    error "Неизвестный пакетный менеджер. Поддерживаются только apt, dnf, yum."
fi

log "Обнаружен пакетный менеджер: $PACKAGE_MANAGER"

# 1. Обновление системы
log "Обновление системы..."
if [ "$PACKAGE_MANAGER" = "apt" ]; then
    sudo apt update -y || warn "Не удалось обновить систему"
    sudo apt upgrade -y || warn "Не удалось обновить пакеты"
elif [ "$PACKAGE_MANAGER" = "dnf" ]; then
    sudo dnf update -y || warn "Не удалось обновить систему"
elif [ "$PACKAGE_MANAGER" = "yum" ]; then
    sudo yum update -y || warn "Не удалось обновить систему"
fi

# 2. Установка необходимых пакетов
log "Установка необходимых пакетов..."

if [ "$PACKAGE_MANAGER" = "apt" ]; then
    # Ubuntu/Debian
    sudo apt install -y python3 python3-pip python3-venv python3-dev || error "Не удалось установить Python"
    sudo apt install -y postgresql postgresql-contrib || error "Не удалось установить PostgreSQL"
    sudo apt install -y build-essential libssl-dev libffi-dev || error "Не удалось установить зависимости"
    sudo apt install -y nginx || error "Не удалось установить Nginx"
    sudo apt install -y git htop curl wget || warn "Не удалось установить дополнительные пакеты"
elif [ "$PACKAGE_MANAGER" = "dnf" ]; then
    # AlmaLinux/RHEL/CentOS 8+
    sudo dnf install -y python3.11 python3.11-pip python3.11-devel || error "Не удалось установить Python"
    sudo dnf install -y postgresql postgresql-server postgresql-contrib || error "Не удалось установить PostgreSQL"
    sudo dnf install -y gcc gcc-c++ make openssl-devel libffi-devel || error "Не удалось установить зависимости"
    sudo dnf install -y nginx || error "Не удалось установить Nginx"
    sudo dnf install -y git htop || warn "Не удалось установить дополнительные пакеты"
elif [ "$PACKAGE_MANAGER" = "yum" ]; then
    # CentOS 7
    sudo yum install -y python3 python3-pip python3-devel || error "Не удалось установить Python"
    sudo yum install -y postgresql postgresql-server postgresql-contrib || error "Не удалось установить PostgreSQL"
    sudo yum install -y gcc gcc-c++ make openssl-devel libffi-devel || error "Не удалось установить зависимости"
    sudo yum install -y nginx || error "Не удалось установить Nginx"
    sudo yum install -y git htop || warn "Не удалось установить дополнительные пакеты"
fi

# 3. Настройка PostgreSQL
log "Настройка PostgreSQL..."

if [ "$PACKAGE_MANAGER" = "apt" ]; then
    # Ubuntu/Debian
    if ! sudo systemctl is-active --quiet postgresql; then
        sudo systemctl start postgresql || error "Не удалось запустить PostgreSQL"
        sudo systemctl enable postgresql || error "Не удалось включить PostgreSQL"
    fi
elif [ "$PACKAGE_MANAGER" = "dnf" ]; then
    # AlmaLinux/RHEL/CentOS 8+
    if ! sudo systemctl is-active --quiet postgresql; then
        sudo postgresql-setup --initdb || error "Не удалось инициализировать PostgreSQL"
        sudo systemctl start postgresql || error "Не удалось запустить PostgreSQL"
        sudo systemctl enable postgresql || error "Не удалось включить PostgreSQL"
    fi
elif [ "$PACKAGE_MANAGER" = "yum" ]; then
    # CentOS 7
    if ! sudo systemctl is-active --quiet postgresql; then
        sudo postgresql-setup initdb || error "Не удалось инициализировать PostgreSQL"
        sudo systemctl start postgresql || error "Не удалось запустить PostgreSQL"
        sudo systemctl enable postgresql || error "Не удалось включить PostgreSQL"
    fi
fi

# Создание пользователя и базы данных
log "Создание пользователя и базы данных..."
sudo -u postgres psql -c "SELECT 1 FROM pg_user WHERE usename='mysite_user'" | grep -q 1 || {
    sudo -u postgres psql -c "CREATE USER mysite_user WITH PASSWORD '12345';" || error "Не удалось создать пользователя БД"
}

sudo -u postgres psql -c "SELECT 1 FROM pg_database WHERE datname='myproject'" | grep -q 1 || {
    sudo -u postgres psql -c "CREATE DATABASE myproject OWNER mysite_user;" || error "Не удалось создать базу данных"
    sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE myproject TO mysite_user;" || error "Не удалось выдать права"
}

# 4. Создание директории проекта
log "Создание директории проекта..."
sudo mkdir -p $PROJECT_DIR || error "Не удалось создать директорию проекта"
sudo chown $USERNAME:$USERNAME $PROJECT_DIR || error "Не удалось изменить владельца директории"

# 5. Копирование файлов проекта
log "Копирование файлов проекта..."
if [ -d "." ]; then
    cp -r . $PROJECT_DIR/ || error "Не удалось скопировать файлы проекта"
else
    error "Директория проекта не найдена. Запустите скрипт из корня проекта."
fi

# 6. Создание виртуального окружения
log "Создание виртуального окружения..."
cd $PROJECT_DIR
python3 -m venv venv || error "Не удалось создать виртуальное окружение"
source venv/bin/activate
pip install --upgrade pip || error "Не удалось обновить pip"
pip install -r requirements.txt || error "Не удалось установить зависимости"

# 7. Создание необходимых директорий
log "Создание необходимых директорий..."
mkdir -p logs static || error "Не удалось создать директории"
chmod 755 logs static

# 8. Создание systemd сервисов
log "Создание systemd сервисов..."

# Сервис для основного приложения
sudo tee /etc/systemd/system/parser-dist.service > /dev/null <<EOF
[Unit]
Description=Parser Distribution Service
After=network.target postgresql.service

[Service]
Type=simple
User=$USERNAME
Group=$USERNAME
WorkingDirectory=$PROJECT_DIR
Environment=PATH=$PROJECT_DIR/venv/bin
ExecStart=$PROJECT_DIR/venv/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Сервис для веб-интерфейса (FastAPI)
sudo tee /etc/systemd/system/parser-web.service > /dev/null <<EOF
[Unit]
Description=Parser Web Interface (FastAPI)
After=network.target

[Service]
Type=simple
User=$USERNAME
Group=$USERNAME
WorkingDirectory=$PROJECT_DIR
Environment=PATH=$PROJECT_DIR/venv/bin
ExecStart=$PROJECT_DIR/venv/bin/uvicorn main_server:app --host 0.0.0.0 --port 8080
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# 9. Активация сервисов
log "Активация сервисов..."
sudo systemctl daemon-reload
sudo systemctl enable parser-dist.service
sudo systemctl enable parser-web.service
sudo systemctl start parser-dist.service
sudo systemctl start parser-web.service

# 10. Настройка Nginx
log "Настройка Nginx..."

# Получение IP адреса сервера
SERVER_IP=$(hostname -I | awk '{print $1}')

sudo tee /etc/nginx/sites-available/parser-dist > /dev/null <<EOF
server {
    listen 80;
    server_name $SERVER_IP;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    location /static/ {
        alias $PROJECT_DIR/static/;
        expires 1d;
        add_header Cache-Control "public, immutable";
    }
}
EOF

# Активация сайта
sudo ln -sf /etc/nginx/sites-available/parser-dist /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default 2>/dev/null || true

# 11. Запуск Nginx
log "Запуск Nginx..."
sudo systemctl enable nginx
sudo systemctl start nginx

# 12. Настройка firewall (Ubuntu/Debian использует ufw)
log "Настройка firewall..."
if command -v ufw &> /dev/null; then
    sudo ufw allow 22/tcp || warn "Не удалось добавить SSH в ufw"
    sudo ufw allow 80/tcp || warn "Не удалось добавить HTTP в ufw"
    sudo ufw allow 443/tcp || warn "Не удалось добавить HTTPS в ufw"
    sudo ufw --force enable || warn "Не удалось включить ufw"
elif command -v firewall-cmd &> /dev/null; then
    sudo firewall-cmd --permanent --add-service=http || warn "Не удалось добавить HTTP в firewall"
    sudo firewall-cmd --permanent --add-service=https || warn "Не удалось добавить HTTPS в firewall"
    sudo firewall-cmd --permanent --add-port=22/tcp || warn "Не удалось добавить SSH в firewall"
    sudo firewall-cmd --reload || warn "Не удалось перезагрузить firewall"
fi

# 13. Создание скрипта резервного копирования
log "Создание скрипта резервного копирования..."
sudo mkdir -p $BACKUP_DIR
sudo chown $USERNAME:$USERNAME $BACKUP_DIR

tee $PROJECT_DIR/backup.sh > /dev/null <<EOF
#!/bin/bash
BACKUP_DIR="$BACKUP_DIR"
DATE=\$(date +%Y%m%d_%H%M%S)

mkdir -p \$BACKUP_DIR

# Резервное копирование базы данных
pg_dump -U mysite_user -h localhost myproject > \$BACKUP_DIR/db_backup_\$DATE.sql

# Резервное копирование файлов приложения
tar -czf \$BACKUP_DIR/app_backup_\$DATE.tar.gz $PROJECT_DIR --exclude=$PROJECT_DIR/venv

# Удаление старых резервных копий (старше 30 дней)
find \$BACKUP_DIR -name "*.sql" -mtime +30 -delete
find \$BACKUP_DIR -name "*.tar.gz" -mtime +30 -delete

echo "Резервное копирование завершено: \$BACKUP_DIR"
EOF

chmod +x $PROJECT_DIR/backup.sh

# 14. Настройка ротации логов
log "Настройка ротации логов..."
sudo tee /etc/logrotate.d/parser-dist > /dev/null <<EOF
$PROJECT_DIR/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 $USERNAME $USERNAME
}
EOF

# 15. Проверка статуса сервисов
log "Проверка статуса сервисов..."
sleep 5

if sudo systemctl is-active --quiet parser-dist.service; then
    log "✓ Сервис parser-dist запущен"
else
    warn "✗ Сервис parser-dist не запущен"
fi

if sudo systemctl is-active --quiet parser-web.service; then
    log "✓ Сервис parser-web запущен"
else
    warn "✗ Сервис parser-web не запущен"
fi

if sudo systemctl is-active --quiet nginx; then
    log "✓ Nginx запущен"
else
    warn "✗ Nginx не запущен"
fi

if sudo systemctl is-active --quiet postgresql; then
    log "✓ PostgreSQL запущен"
else
    warn "✗ PostgreSQL не запущен"
fi

# 16. Финальная информация
log "Развертывание завершено!"
echo ""
echo "=== ИНФОРМАЦИЯ О РАЗВЕРТЫВАНИИ ==="
echo "Проект размещен в: $PROJECT_DIR"
echo "Веб-интерфейс доступен по адресу: http://$SERVER_IP"
echo "Прямой доступ к приложению: http://$SERVER_IP:8080"
echo ""
echo "=== УПРАВЛЕНИЕ СЕРВИСАМИ ==="
echo "Проверить статус: sudo systemctl status parser-dist.service parser-web.service nginx postgresql"
echo "Перезапустить: sudo systemctl restart parser-dist.service parser-web.service"
echo "Просмотр логов: sudo journalctl -u parser-dist.service -f"
echo ""
echo "=== РЕЗЕРВНОЕ КОПИРОВАНИЕ ==="
echo "Ручное резервное копирование: $PROJECT_DIR/backup.sh"
echo "Автоматическое резервное копирование: добавьте в crontab: 0 2 * * * $PROJECT_DIR/backup.sh"
echo ""
echo "=== ПОЛЕЗНЫЕ КОМАНДЫ ==="
echo "Мониторинг ресурсов: htop"
echo "Проверка портов: sudo netstat -tlnp"
if command -v ufw &> /dev/null; then
    echo "Проверка firewall: sudo ufw status"
else
    echo "Проверка firewall: sudo firewall-cmd --list-all"
fi
echo ""

# Проверка доступности веб-интерфейса
log "Проверка доступности веб-интерфейса..."
if curl -s http://localhost:8080 > /dev/null; then
    log "✓ Веб-интерфейс доступен"
else
    warn "✗ Веб-интерфейс недоступен. Проверьте логи: sudo journalctl -u parser-web.service"
fi

log "Развертывание завершено успешно!" 