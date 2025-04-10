# Introducción

Solución de control online en tiempo real "EasyCab".

Usando sockets.

Python, Pygame, SQLite, Kafka.

Hay un mapa de 20x20 posiciones (Matriz 2D)

![[Pasted image 20250410161023.png]]
Habrán taxis representados por un ID y estado dependiedno del color.

Habrán Customers que pueden solicitar un servicio (ID=Letra minúscula).

Primero se ejecuta EC_CENTRAL.py.

Lee un fichero del mapa.

Tiene una BD (SQLite) con los taxis.

Los clientes podrán solicitar un servicio desde su aplicacion (Para automatizar, los servicios estáran en un fichero).

# Estructura

## EC_Central.py

### Parámetros
Puerto de escucha, IP y puerto del Broker, IP y puerto de BD.

### Funcionamiento
Al ejecutar el .py se mostrará un mapa de 20x20 recogido de un fichero con los taxis (BD) en pygame y cada movimiento del taxi se actualizará.

### Base de datos
Contendra ID, estado y posición (SQLite).

## EC_DE.py y EC_S.py

## EC_Customer.py
