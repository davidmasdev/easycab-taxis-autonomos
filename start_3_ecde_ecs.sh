#!/bin/bash

# Lanzar tres EC_DE en puertos fijos
python3 EC_DE.py 1 localhost:3030 localhost:9092  &
python3 EC_DE.py 2 localhost:3030 localhost:9092  &
python3 EC_DE.py 3 localhost:3030 localhost:9092  &

# Esperar unos segundos para que los EC_DE abran sus puertos
sleep 3

# Lanzar tres EC_S conectados a los puertos 6000, 6001 y 6002
python3 EC_S.py localhost:6000 &
python3 EC_S.py localhost:6001 &
python3 EC_S.py localhost:6002 &

wait 