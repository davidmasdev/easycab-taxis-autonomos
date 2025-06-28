#!/bin/bash

# Script mejorado para iniciar todas las aplicaciones con verificaciones de conectividad

echo "=== EasyCab System Startup ==="
echo "Verificando servicios..."

# Función para verificar si un puerto está disponible
check_port() {
    local host=$1
    local port=$2
    local service=$3
    local max_attempts=30
    local attempt=1
    
    echo "Verificando $service en $host:$port..."
    while [ $attempt -le $max_attempts ]; do
        if nc -z $host $port 2>/dev/null; then
            echo "✓ $service está disponible en $host:$port"
            return 0
        fi
        echo "  Intento $attempt/$max_attempts: $service no disponible, esperando..."
        sleep 2
        ((attempt++))
    done
    echo "✗ $service no está disponible después de $max_attempts intentos"
    return 1
}

# Verificar servicios base
echo "1. Verificando servicios base..."
if ! check_port "localhost" "9092" "Kafka"; then
    echo "Error: Kafka no está disponible. Asegúrate de que docker-compose esté ejecutándose."
    exit 1
fi

if ! check_port "localhost" "3306" "MySQL"; then
    echo "Error: MySQL no está disponible. Asegúrate de que docker-compose esté ejecutándose."
    exit 1
fi

if ! check_port "localhost" "1234" "EC_Registry"; then
    echo "Advertencia: EC_Registry no está disponible. Algunas funciones pueden no funcionar correctamente."
fi

echo "2. Iniciando EC_Central..."
python3 EC_Central.py &
CENTRAL_PID=$!
sleep 5

# Verificar que EC_Central esté funcionando
if ! check_port "localhost" "3030" "EC_Central"; then
    echo "Error: EC_Central no se inició correctamente"
    kill $CENTRAL_PID 2>/dev/null
    exit 1
fi

if ! check_port "localhost" "7070" "EC_Central API"; then
    echo "Advertencia: EC_Central API no está disponible"
fi

echo "3. Iniciando taxis..."
echo "  Iniciando Taxi 1..."
python3 EC_DE.py 1 localhost:3030 localhost:9092 &
TAXI1_PID=$!
sleep 3

echo "  Iniciando EC_S para Taxi 1..."
python3 EC_S.py localhost:6000 &
ECS1_PID=$!
sleep 3

echo "  Iniciando Taxi 2..."
python3 EC_DE.py 2 localhost:3030 localhost:9092 &
TAXI2_PID=$!
sleep 3

echo "  Iniciando EC_S para Taxi 2..."
python3 EC_S.py localhost:6001 &
ECS2_PID=$!
sleep 3

echo "4. Iniciando clientes..."
echo "  Iniciando Cliente A..."
python3 EC_Customer.py a localhost:9092 &
CUSTOMER_A_PID=$!
sleep 2

echo "  Iniciando Cliente B..."
python3 EC_Customer.py b localhost:9092 &
CUSTOMER_B_PID=$!
sleep 2

echo "  Iniciando Cliente C..."
python3 EC_Customer.py c localhost:9092 &
CUSTOMER_C_PID=$!
sleep 2

echo "=== Sistema iniciado correctamente ==="
echo "PIDs de los procesos:"
echo "  EC_Central: $CENTRAL_PID"
echo "  Taxi 1 (EC_DE): $TAXI1_PID"
echo "  Taxi 1 (EC_S): $ECS1_PID"
echo "  Taxi 2 (EC_DE): $TAXI2_PID"
echo "  Taxi 2 (EC_S): $ECS2_PID"
echo "  Cliente A: $CUSTOMER_A_PID"
echo "  Cliente B: $CUSTOMER_B_PID"
echo "  Cliente C: $CUSTOMER_C_PID"

echo ""
echo "Para detener todos los procesos, ejecuta:"
echo "kill $CENTRAL_PID $TAXI1_PID $ECS1_PID $TAXI2_PID $ECS2_PID $CUSTOMER_A_PID $CUSTOMER_B_PID $CUSTOMER_C_PID"

# Función para limpiar procesos al salir
cleanup() {
    echo ""
    echo "Deteniendo todos los procesos..."
    kill $CENTRAL_PID $TAXI1_PID $ECS1_PID $TAXI2_PID $ECS2_PID $CUSTOMER_A_PID $CUSTOMER_B_PID $CUSTOMER_C_PID 2>/dev/null
    echo "Sistema detenido."
    exit 0
}

# Capturar señales para limpiar procesos
trap cleanup SIGINT SIGTERM

echo ""
echo "Presiona Ctrl+C para detener todos los procesos"
echo "Esperando..."

# Esperar a que todos los procesos terminen
wait 