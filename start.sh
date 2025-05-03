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

# Создаем общую сеть (если её нет)
NETWORK_NAME="my-global-network"
if ! docker network inspect "$NETWORK_NAME" &> /dev/null; then
    echo "🌐 Создаем сеть $NETWORK_NAME..."
    docker network create "$NETWORK_NAME"
fi

# Создаем общий том для DAG-файлов (если его нет)
VOLUME_NAME="airflow_dags"
if ! docker volume inspect "$VOLUME_NAME" &> /dev/null; then
    echo "💾 Создаем общий том $VOLUME_NAME..."
    docker volume create "$VOLUME_NAME"
fi

# Функция для запуска сервиса
start_service() {
    local service_name=$1
    local compose_file=$2

    echo "🔄 Запуск $service_name..."
    cd "$service_name" || { echo "❌ Папка $service_name не найдена"; exit 1; }
    
    # Запускаем контейнеры с указанием сети
    docker-compose -f "$compose_file" up -d
    
    # Подключаем все контейнеры сервиса к общей сети
    for container in $(docker-compose -f "$compose_file" ps -q); do
        docker network connect "$NETWORK_NAME" "$container" &> /dev/null || true
    done
    
    cd ..
    echo "✅ $service_name запущен и подключен к сети $NETWORK_NAME"
}

# Запускаем все сервисы
start_service "hadoop" "docker-compose.yml"
start_service "airflow" "docker-compose.yml"
start_service "jenkins" "docker-compose.yml"
start_service "application spark" "docker-compose.yml"

echo "🎉 Все сервисы успешно запущены и соединены в сети $NETWORK_NAME!"
echo "🔹 Hadoop: http://localhost:9870"
echo "🔹 Airflow: http://localhost:8080 (логин: admin, пароль: admin)"
echo "🔹 Jenkins: http://localhost:9090 (пароль из logs: docker logs jenkins)"
echo "🔹 Spark Master: http://localhost:8081"