#!/bin/bash

python3 EC_Customer.py a &
python3 EC_Customer.py b &
python3 EC_Customer.py c &

wait 