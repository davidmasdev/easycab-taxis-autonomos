import sys
import socket
from kafka import KafkaConsumer, KafkaProducer

ipPortCentral = sys.argv[1]
ipPortBroker = sys.argv[2]
ipPortEcs = sys.argv[3]
idTaxi = sys.argv[4]

ipCentral = ipPortCentral.split(":")[0]
portCentral = int(ipPortCentral.split(":")[1])

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((ipCentral, portCentral))
    s.sendall(f"{idTaxi}".encode())
    data = s.recv(1024)
    print(f"Received {data.decode()} from Central")