import sys
import socket
from kafka import KafkaConsumer, KafkaProducer
import pygame

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

# # Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Kafka Consumer
consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
consumer.subscribe(['centralTaxi'])

# This sets the WIDTH and HEIGHT of each grid location
WIDTH = 20
HEIGHT = 20

# This sets the margin between each cell
MARGIN = 5

# pygame setup
pygame.init()
screen = pygame.display.set_mode((1280, 720))
pygame.display.set_caption(f'EasyCab | EC_DE Taxi {idTaxi}')
clock = pygame.time.Clock()
font = pygame.font.SysFont('arial', 24)
running = True

grid = []
client = ''
clientPosition = []
destination = []
destinationPosition = []
position = []

while running:
    # pygame.QUIT event means the user clicked X to close your window
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    msg = consumer.poll(timeout_ms=100)

    if msg:
        for tp, records in msg.items():
            for record in records:
                message = record.value.decode()
                messageSplit = message.split(" ")
                if(message.strip().startswith('grid')):
                    grid = eval(message.strip()[5:])
                if(messageSplit[0] == 'asignar' and messageSplit[1] == str(idTaxi)):
                    client = messageSplit[3]
                    print(client)
                    destination = messageSplit[5]
                    print(destination)
                   
    # RENDER YOUR GAME HERE
    if grid != []:
        for row in range(20):
            for column in range(20):
                rect = pygame.Rect([(MARGIN + WIDTH) * column + MARGIN,
                                    (MARGIN + HEIGHT) * row + MARGIN,
                                    WIDTH,
                                    HEIGHT])

                color = "white"
                if (grid[row][column] != 0):
                    if (isinstance(grid[row][column], int)):
                        if (grid[row][column] == int(idTaxi)):
                            position = [row, column]
                        color = "green"
                    elif (grid[row][column].islower()):
                        if (grid[row][column] == client):
                            clientPosition = [row, column]
                        color = "yellow"
                    else:
                        if (grid[row][column] == destination):
                            destinationPosition = [row, column]
                        color = "light blue"
                pygame.draw.rect(screen, color, rect)
                if (grid[row][column] != 0):
                    text_surface = font.render(str(grid[row][column]), True, "black")
                    text_rect = text_surface.get_rect(center=rect.center)
                    screen.blit(text_surface, text_rect)

    print(f"position {position} clientPosition {clientPosition} destinationPosition {destinationPosition}")
    if position != clientPosition and position != [] and clientPosition != []:
        print(f"Taxi {idTaxi} moviendo a {client} hacia {destination}")
        if position[0] < clientPosition[0]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0]+1, position[1]]}').encode())
        elif position[0] > clientPosition[0]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0]-1, position[1]]}').encode())
        elif position[1] < clientPosition[1]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0], position[1]+1]}').encode())
        elif position[1] > clientPosition[1]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0], position[1]-1]}').encode()) 

    # flip() the display to put
    #  your work on screen
    pygame.display.flip()

    clock.tick(1)


consumer.close()
pygame.quit()

