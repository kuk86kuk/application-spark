#!/bin/bash

# Проверяем, установлен ли Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker не установлен. Установите Docker перед запуском."
    exit 1
fi

# Проверяем, установлен ли Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose не установлен. Установите его."
    exit 1
fi

# Функция для запуска сервиса
start_service() {
    local service_name=$1
    local compose_file=$2

    echo "🔄 Запуск $service_name..."
    cd "$service_name" || { echo "❌ Папка $service_name не найдена"; exit 1; }
    docker-compose -f "$compose_file" up -d
    cd ..
    echo "✅ $service_name запущен"
}

# Запускаем все сервисы
start_service "hadoop" "docker-compose.yml"
start_service "airflow" "docker-compose.yml"
start_service "jenkins" "docker-compose.yml"
start_service "application spark" "docker-compose.yml"

echo "🎉 Все сервисы успешно запущены!"
echo "🔹 Hadoop: http://localhost:9870"
echo "🔹 Airflow: http://localhost:8080"
echo "🔹 Jenkins: http://localhost:9090"
echo "🔹 application spark Master: http://localhost:8081"