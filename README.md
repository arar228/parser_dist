<div align="center">
  <h1>🚀 B2B API Aggregator & Parser</h1>
  <p><strong>A powerful, asynchronous aggregation system for price lists and stock levels from leading IT distributors</strong></p>
  <p>
    <a href="#-english">🇺🇸 English</a> | <a href="#-русский">🇷🇺 Русский</a>
  </p>
  <img src="https://img.shields.io/badge/Python-3.10+-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/FastAPI-0.100+-green.svg" alt="FastAPI">
  <img src="https://img.shields.io/badge/MySQL-8.0-blue.svg" alt="MySQL">
  <img src="https://img.shields.io/badge/Asyncio-Enabled-orange.svg" alt="Asyncio">
  <img src="https://img.shields.io/badge/Status-Production%20Ready-success.svg" alt="Status">
</div>

<br>

<h2 id="-english">🇺🇸 English</h2>

## 🌟 About the Project

One of the main challenges in IT and e-commerce is maintaining up-to-date pricing and stock data from dozens of suppliers, each with their own unique APIs and data formats. 
**B2B API Aggregator** solves this problem by combining leading IT distributors into a single, consistent, and ultra-fast database with its own REST API.

The system automatically collects hundreds of thousands of items, normalizes part numbers, converts currencies according to current exchange rates, and delivers lightning-fast data to your clients or online store.

## 🔥 Key Features

- 🔌 **Out-of-the-Box Integrations**:
  - `Merlion` (SOAP/REST API)
  - `Treolan` (B2B API)
  - `OCS Distribution` (REST API)
  - `Marvel` (REST API)
  - `VVP Group` (REST API)
  - `Netlab` (SOAP API)
  - `Resursmedio` (SOAP/REST)
- ⚡ **High Performance**: Built with `asyncio`, `AIOHTTP`, and optimized SQL queries (`ON DUPLICATE KEY UPDATE`) for instant processing of massive catalogs.
- 🗄️ **Smart Data Storage**: Robust MySQL relational structure with well-designed indexes for instantaneous lookups by SKU, brand, and part number.
- 💵 **Multi-Currency Support**: Automatic extraction and conversion of prices (RUB, USD, EUR) with support for custom exchange rates.
- 🤖 **Telegram Notifications**: Built-in monitoring system — get direct alerts in Telegram for successful data syncs, new products, and potential system errors.
- 🐳 **Easy Deployment**: Ready-to-use bash scripts (`deploy.sh`, `update_server.sh`) for automated deployment on Linux servers (Ubuntu, AlmaLinux).

## 🛠️ Architecture

The project is built on a modern tech stack ensuring 24/7 reliability:
* **Backend:** Python + FastAPI (high-performance async framework)
* **Database:** MySQL (via SQLAlchemy with the async `aiomysql` driver)
* **Tasks & Scheduler:** Built-in task scheduling system for background synchronization of supplier data without blocking the main API.

## 🚀 Quick Start

### Requirements
- Python 3.10+
- MySQL 8.0+

### Installation

1. Clone the repository:
```bash
git clone https://github.com/your_username/your_repository.git
cd b2b-api-aggregator
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure the application:
Rename `.env.example` (if available) or create an `.env` file in the root directory to set your credentials (templates are available in configuration files). Ensure your Database details and API keys are set.

4. Run the server:
```bash
python main.py
```
The API will be available at `http://localhost:8080` (by default). The auto-generated Swagger UI documentation will be available at: `http://localhost:8080/docs`.

## 🔒 Security & Privacy

The entire codebase is designed with security best practices in mind:
- Authorization mechanisms and session management.
- Complete isolation of credentials from the codebase.
- Memory-safe streaming when processing giant XML/JSON responses from suppliers to prevent crashes.

---

<br>
<br>

<h2 id="-русский">🇷🇺 Русский</h2>

## 🌟 О проекте

Один из главных вызовов в IT и e-commerce — это поддержание актуальных цен и остатков от десятков поставщиков с уникальными API и форматами данных. 
**B2B API Aggregator** решает эту проблему, объединяя ведущих IT-дистрибьюторов в единую, консистентную и сверхбыструю базу данных с собственным REST API.

Система автоматически собирает сотни тысяч позиций, нормализует артикулы, конвертирует валюты по актуальному курсу и предоставляет вашим клиентам или интернет-магазину данные с молниеносной скоростью.

## 🔥 Ключевые возможности

- 🔌 **Интеграция с топовыми дистрибьюторами «из коробки»**:
  - `Merlion` (SOAP/REST API)
  - `Treolan` (B2B API)
  - `OCS Distribution` (REST API)
  - `Marvel` (REST API)
  - `VVP Group` (REST API)
  - `Netlab` (SOAP API)
  - `Resursmedio` (SOAP/REST)
- ⚡ **Высокая производительность**: Использование `asyncio`, `AIOHTTP` и оптимизированных SQL-запросов (ON DUPLICATE KEY UPDATE) для мгновенного обновления сотен тысяч позиций.
- 🗄️ **Smart Data Storage**: Мощная реляционная структура MySQL с грамотными индексами для моментального поиска по артикулам, брендам и part-номерам.
- 💵 **Мультивалютность**: Автоматический парсинг и конвертация цен (RUB, USD, EUR) с поддержкой кастомных курсов.
- 🤖 **Telegram-уведомления**: Встроенная система мониторинга — получайте отчеты об успешных выгрузках, новых товарах и возможных ошибках напрямую в Telegram.
- 🐳 **Легкий деплой**: Готовые bash-скрипты (`deploy.sh`, `update_server.sh`) для автоматического развертывания на Linux-серверах (Ubuntu, AlmaLinux).

## 🛠️ Архитектура

Проект построен на передовом стеке технологий, обеспечивающем надежность 24/7:
* **Backend:** Python + FastAPI (асинхронный высокопроизводительный фреймворк)
* **Database:** MySQL (через SQLAlchemy с асинхронным драйвером aiomysql)
* **Tasks & Scheduler:** Встроенная система планирования задач для фонового обновления баз поставщиков без блокировки основного API.

## 🚀 Быстрый старт

### Требования
- Python 3.10+
- MySQL 8.0+

### Установка

1. Склонируйте репозиторий:
```bash
git clone https://github.com/твое_имя/твой_репозиторий.git
cd b2b-api-aggregator
```

2. Установите зависимости:
```bash
pip install -r requirements.txt
```

3. Настройте конфигурацию:
Переименуйте `.env.example` (если есть) или создайте файл `.env` в корневой директории и укажите доступы (шаблон доступен в конфигурационных файлах). Настройте данные для БД и API ключи дистрибьюторов.

4. Запустите миграции и сервер:
```bash
python main.py
```
API будет доступно по адресу `http://localhost:8080` (по умолчанию). Автоматически сгенерированная документация Swagger UI: `http://localhost:8080/docs`.

## 🔒 Безопасность и Конфиденциальность

Весь код спроектирован с учетом лучших практик безопасности:
- Механизмы авторизации и управления сессиями.
- Полная изоляция учетных данных от кодовой базы.
- Защита от перегрузки памяти (Memory-safe streaming) при обработке гигантских XML/JSON ответов поставщиков.

---

<div align="center">
  <b>Разработано с ❤️ для автоматизации бизнеса</b>
</div>
