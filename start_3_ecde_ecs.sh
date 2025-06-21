#!/bin/bash

python3 EC_DE.py 1 localhost:3030 localhost:9092  &
sleep 3
python3 EC_S.py localhost:6000 &
sleep 3
python3 EC_DE.py 2 localhost:3030 localhost:9092  &
sleep 3
python3 EC_S.py localhost:6001 &

wait 