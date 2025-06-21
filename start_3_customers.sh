#!/bin/bash

python3 EC_Customer.py a &
sleep 1
python3 EC_Customer.py b &
sleep 1
python3 EC_Customer.py c &

wait 