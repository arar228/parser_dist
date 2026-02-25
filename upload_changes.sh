#!/bin/bash
# Скрипт для загрузки всех измененных файлов на сервер

echo "🚀 Загружаем измененные файлы на сервер office-srv..."

# Основные файлы
echo "📄 Загружаем main.py..."
scp main.py dc-srv@office-srv:~/parser_dist/main.py

echo "📄 Загружаем core/upsert.py..."
scp core/upsert.py dc-srv@office-srv:~/parser_dist/core/upsert.py

echo "📄 Загружаем core/db_create_tables.py..."
scp core/db_create_tables.py dc-srv@office-srv:~/parser_dist/core/db_create_tables.py

# Модули дистрибьюторов
echo "📄 Загружаем merlion/merlion_downloader.py..."
scp merlion/merlion_downloader.py dc-srv@office-srv:~/parser_dist/merlion/merlion_downloader.py

echo "📄 Загружаем vvp/vvp_export.py..."
scp vvp/vvp_export.py dc-srv@office-srv:~/parser_dist/vvp/vvp_export.py

echo "📄 Загружаем treolan/treolan_export.py..."
scp treolan/treolan_export.py dc-srv@office-srv:~/parser_dist/treolan/treolan_export.py

echo "📄 Загружаем ocs/ocs_export.py..."
scp ocs/ocs_export.py dc-srv@office-srv:~/parser_dist/ocs/ocs_export.py

echo "📄 Загружаем ocs/ocs_export_backup.py..."
scp ocs/ocs_export_backup.py dc-srv@office-srv:~/parser_dist/ocs/ocs_export_backup.py

# Тестовый скрипт
echo "📄 Загружаем test_system_functionality.py..."
scp test_system_functionality.py dc-srv@office-srv:~/parser_dist/test_system_functionality.py

echo "✅ Все файлы загружены на сервер!"
echo ""
echo "🔧 Для проверки на сервере выполните:"
echo "ssh dc-srv@office-srv"
echo "cd ~/parser_dist"
echo "python test_system_functionality.py"
echo ""
echo "🚀 Для запуска парсера:"
echo "python main.py  # Основной режим с почасовыми обновлениями"
echo "python main.py update_vvp  # Обновить только VVP"
echo "python main.py update_merlion  # Обновить только Merlion" 