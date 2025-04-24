import pygame
import sys
import sqlite3
from kafka import KafkaConsumer, KafkaProducer
import random
import socket

# Params
listenPort = sys.argv[1]
dataBroker = sys.argv[2]
database = sys.argv[3]
locationParam = sys.argv[4]

# Locations treatment
f = open(locationParam)
rawLocations = f.read()
unformatedLocations = rawLocations.split()
locations = []
for location in unformatedLocations:
    newLocation = location.split("|")
    locations.append([newLocation[0], [newLocation[1], newLocation[2]]])

# Database treatment
con = sqlite3.connect(database)
cur = con.cursor()
cur.execute('Create table if not exists taxis (id integer primary key autoincrement, estado string, posX integer, posY integer, destino string)')
cur.execute('Insert into taxis (estado, posX, posY, destino) values (?, ?, ?, ?)', ('Ok', 0, 0, ''))

# This sets the WIDTH and HEIGHT of each grid location
WIDTH = 20
HEIGHT = 20

# This sets the margin between each cell
MARGIN = 5

# pygame setup
pygame.init()
screen = pygame.display.set_mode((1280, 720))
pygame.display.set_caption("EasyCab | EC_Central")
clock = pygame.time.Clock()
font = pygame.font.SysFont('arial', 24)
running = True

# Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

producer.send('taxiCentral', (f'').encode())

# Kafka Consumer
consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
consumer.subscribe(['customerCentral', 'taxiCentral'])

customers = []

# Socket creation
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('127.0.0.1', int(listenPort)))
s.listen(1)
s.setblocking(0)

# Game loop
while running:
    # poll for events
    # pygame.QUIT event means the user clicked X to close your window
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    # fill the screen with a color to wipe away anything from last frame
    screen.fill("grey")

    # Get taxis from db
    res = cur.execute("Select * from taxis")
    taxis = res.fetchall()

    # Get taxis from socket connection
    try:
        conn, addr = s.accept()
        conn.setblocking(0)
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            for taxi in taxis:
                if str(taxi[0]) == data:
                    print(f"Taxi {data} autenticado y operativo")
                    conn.sendall(("Taxi autenticado").encode())            
        conn.close()
    except BlockingIOError:
        pass

    # Grid treatment
    grid = []
    locationFound = False

    for row in range(20):
        grid.append([])
        for column in range(20):
            locationFound = False
            for location in locations:
                if(location[1] == [str(row), str(column)]):
                    grid[row].append(location[0])
                    locationFound = True
            for taxi in taxis:
                if([taxi[2], taxi[3]] == [row, column]):
                    grid[row].append(taxi[0])
            for customer in customers:
                if(customer[2] == [row, column]):
                    grid[row].append(customer[0])
            if(locationFound == False):
                grid[row].append(0)

    msg = consumer.poll(timeout_ms=100)

    if msg:
        for tp, records in msg.items():
            for record in records:
                # print(record)
                message = record.value.decode()
                messageSplit = record.value.decode().split(' ')
                if(messageSplit[0] == 'create'):
                    posX = random.choice([x for x in range(20) if x not in [subarray[1][0] for subarray in locations]])
                    posY = random.choice([y for y in range(20) if y not in [subarray[1][1] for subarray in locations]])
                    customers.append([messageSplit[2], '', [posX, posY]])
                    grid[posX][posY] = messageSplit[2]
                    print(f'Creado customer {messageSplit[2]} en posición {[posX, posY]}')

                if(messageSplit[0] == 'service'):
                    for customer in customers:
                        if customer[0] == messageSplit[3]:
                            customer[1] = messageSplit[1]
                            print(f'Asignado servicio {customer[1]} al Customer {customer[0]}')
                            for taxi in taxis:
                                print(taxi)
                                if(taxi[4] == '' and taxi[1] == 'Ok'): # TODO: Cambiar en un futuro
                                    cur.execute('update taxis set destino = ?, estado = ? where id = ?', (customer[1], f'OK. Servicio {customer[0]}', taxi[0]))
                                    producer.send('centralTaxi', (f'asignar {taxi[0]} cliente {customer[0]} destino {customer[1]}').encode())
                                    print(f'Customer {customer[0]} con servicio a {customer[1]} asignado al taxi {taxi[0]}')
                
                if(messageSplit[0] == 'mover'):
                    for taxi in taxis:
                        if str(taxi[0]) == messageSplit[1]:
                            newPos = eval(message[message.find('['):message.find(']')+1])
                            print(newPos[0], newPos[1])
                            cur.execute('update taxis set posX = ?, posY = ? where id = ?', (newPos[0], newPos[1], taxi[0]))
                            # grid[int(messageSplit[2])][int(messageSplit[3])] = taxi[0]
                            # grid[int(taxi[2])][int(taxi[3])] = 0
                            # print(f'Taxi {taxi[0]} movido a {messageSplit[2]}, {messageSplit[3]}')
                            # producer.send('centralTaxi', (f'mover {taxi[0]} {messageSplit[2]} {messageSplit[3]}').encode())

    producer.send('centralTaxi', (f'grid {str(grid)}').encode())

    # RENDER YOUR GAME HERE
    for row in range(20):
        for column in range(20):
            rect = pygame.Rect([(MARGIN + WIDTH) * column + MARGIN,
                                (MARGIN + HEIGHT) * row + MARGIN,
                                WIDTH,
                                HEIGHT])

            color = "white"
            if (grid[row][column] != 0):
                if (isinstance(grid[row][column], int)):
                    color = "green"
                elif (grid[row][column].islower()):
                    color = "yellow"
                else:
                    color = "light blue"
            pygame.draw.rect(screen, color, rect)

            if (grid[row][column] != 0):
                text_surface = font.render(str(grid[row][column]), True, "black")
                text_rect = text_surface.get_rect(center=rect.center)
                screen.blit(text_surface, text_rect)

    # flip() the display to put your work on screen
    pygame.display.flip()

    clock.tick(60)  # limits FPS to 60

con.execute('drop table taxis')
pygame.quit()
con.close()
consumer.close()
