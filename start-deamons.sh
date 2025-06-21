#!/bin/bash

# Detectar la IP del host
HOST_IP=$(hostname -I | awk '{print $1}')

echo "Detected host IP: $HOST_IP"
echo "Starting Kafka with KAFKA_HOST=$HOST_IP"
echo "Starting Frontend with FRONTEND_API_HOST=$HOST_IP"

# Exportar la variable de entorno y ejecutar docker-compose
export KAFKA_EXTERNAL_HOST=$HOST_IP
export FRONTEND_API_HOST=$HOST_IP
docker-compose up -d