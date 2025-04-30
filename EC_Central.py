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
cur.execute('Insert into taxis (estado, posX, posY, destino) values (?, ?, ?, ?)', ('Sin autenticar', 0, 0, ''))
cur.execute('Insert into taxis (estado, posX, posY, destino) values (?, ?, ?, ?)', ('Sin autenticar', 0, 0, ''))
cur.execute('Insert into taxis (estado, posX, posY, destino) values (?, ?, ?, ?)', ('Sin autenticar', 0, 0, ''))
cur.execute('Insert into taxis (estado, posX, posY, destino) values (?, ?, ?, ?)', ('Sin autenticar', 0, 0, ''))

# This sets the WIDTH and HEIGHT of each grid location
WIDTH = 20
HEIGHT = 20

# This sets the margin between each cell
MARGIN = 5

# pygame setup
pygame.init()
screen = pygame.display.set_mode((1280, 720), pygame.RESIZABLE)
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
                    cur.execute('update taxis set estado = ? where id = ?', ('OK', taxi[0]))
                    conn.sendall(("Taxi autenticado").encode())
        conn.close()
    except BlockingIOError:
        pass

    # Grid treatment
    grid = []
    locationFound = False

    grid = [[0 for _ in range(20)] for _ in range(20)]

    for row in range(20):
        for column in range(20):
            for location in locations:
                if location[1] == [str(row), str(column)]:
                    grid[row][column] = location[0]
            for taxi in taxis:
                if [taxi[2], taxi[3]] == [row, column]:
                    grid[row][column] = taxi[0]
            for customer in customers:
                if customer[2] == [row, column]:
                    if isinstance(grid[row][column], int) and grid[row][column] != 0:
                        grid[row][column] = str(grid[row][column]) + customer[0]
                    else:
                        grid[row][column] = customer[0]

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
                            asigned = False
                            customer[1] = messageSplit[1]
                            # print(f'Asignado servicio {customer[1]} al Customer {customer[0]}')
                            for taxi in taxis:
                                print(taxi)
                                if(taxi[4] == '' and taxi[1] == 'OK'): # TODO: Cambiar en un futuro
                                    cur.execute('update taxis set destino = ?, estado = ? where id = ?', (customer[1], f'OK. Servicio {customer[0]}', taxi[0]))
                                    producer.send('centralTaxi', (f'asignar {taxi[0]} cliente {customer[0]} destino {customer[1]}').encode())
                                    producer.send('centralCustomer', (f'aceptado {customer[0]} {customer[1]}').encode())
                                    print(f'Customer {customer[0]} con servicio a {customer[1]} asignado al taxi {taxi[0]}')
                                    asigned = True
                                    break
                            if not asigned:
                                producer.send('centralCustomer', (f'denegacion {customer[0]} {messageSplit[1]} de servicio').encode())

                if(messageSplit[0] == 'mover'):
                    for taxi in taxis:
                        print(messageSplit)
                        if messageSplit[1] == 'juntos' and str(taxi[0]) == messageSplit[3]:
                            newPos = eval(message[message.find('['):message.find(']')+1])
                            print(newPos[0], newPos[1])
                            cur.execute('update taxis set posX = ?, posY = ? where id = ?', (newPos[0], newPos[1], taxi[0]))
                            for customer in customers:
                                if customer[0] == messageSplit[2]:
                                    customer[2] = [newPos[0], newPos[1]]
                                    print(f'Customer {customer[0]} moved to position {newPos}')

                        elif str(taxi[0]) == messageSplit[1]:
                            newPos = eval(message[message.find('['):message.find(']')+1])
                            print(newPos[0], newPos[1])
                            cur.execute('update taxis set posX = ?, posY = ? where id = ?', (newPos[0], newPos[1], taxi[0]))
                
                if(messageSplit[0] == 'terminado'):
                    producer.send('centralCustomer', (f'servicio terminado {messageSplit[1]} {messageSplit[2]}').encode())
                    cur.execute('update taxis set estado = ?, destino = ? where id = ?', ('OK', '', messageSplit[3]))
                    for customer in customers:
                        if customer[0] == messageSplit[1]:
                            customer[1] = ''
                            print(f'Servicio {messageSplit[1]} terminado')
                if(messageSplit[0] == 'terminados'):
                    for customer in customers:
                        if customer[0] == messageSplit[1]:
                            customer[1] = 'terminado'
                            print(f'Custoemr {messageSplit[0]} ha terminado todos los servicios')

    producer.send('centralTaxi', (f'grid {str(grid)}').encode())
    producer.send('centralTaxi', (f'taxis {str(taxis)}').encode())
    producer.send('centralTaxi', (f'customers {str(customers)}').encode())
    producer.send('centralTaxi', (f'locations {str(locations)}').encode())
    
    # RENDER YOUR GAME HERE
    grid_width = 20 * (WIDTH + MARGIN) + MARGIN
    grid_height = 20 * (HEIGHT + MARGIN) + MARGIN

    # Calculate offsets for centering in y and adding margin in x
    x_offset = 50  # Margin in x
    # Dynamically calculate cell size based on screen size
    grid_width = screen.get_width() - 100  # Leave some margin in x
    grid_height = screen.get_height() - 100  # Leave some margin in y

    cell_width = (grid_width - (MARGIN * 21)) // 20
    cell_height = (grid_height - (MARGIN * 21)) // 20

    # Ensure cells are square and fit within the screen
    cell_size = min(cell_width, cell_height)
    WIDTH = HEIGHT = max(20, cell_size)  # Minimum size of 20

    # Recalculate grid dimensions
    grid_width = 20 * (WIDTH + MARGIN) + MARGIN
    grid_height = 20 * (HEIGHT + MARGIN) + MARGIN

    # Calculate offsets for centering in y and adding margin in x
    x_offset = 50  # Margin in x
    y_offset = (screen.get_height() - grid_height) // 2  # Dynamically centered in y

    for row in range(20):
        for column in range(20):
            rect = pygame.Rect([(MARGIN + WIDTH) * column + MARGIN + x_offset,
                                (MARGIN + HEIGHT) * row + MARGIN + y_offset,
                                WIDTH,
                                HEIGHT])

            color = "white"
            if (grid[row][column] != 0):
                if (isinstance(grid[row][column], int)):
                    for taxi in taxis:
                        if taxi[0] == grid[row][column]:
                            if taxi[1].startswith('OK'):
                                color = "green"
                            else:
                                color = "red"
                elif (grid[row][column].islower()):
                    color = "yellow"
                else:
                    color = "light blue"
            pygame.draw.rect(screen, color, rect)

            if (grid[row][column] != 0):
                text_surface = font.render(str(grid[row][column]), True, "black")
                text_rect = text_surface.get_rect(center=rect.center)
                screen.blit(text_surface, text_rect)

    # Render taxi and customer information
    info_width = max(450, screen.get_width() // 3)  # Dynamically adjust width based on screen size
    info_x = grid_width + (screen.get_width() - grid_width - info_width) // 2  # Center horizontally between grid and right edge
    info_panel_rect = pygame.Rect(info_x - 20, 50, info_width, screen.get_height() - 80)  # Increase height to prevent overflow
    pygame.draw.rect(screen, "lightgrey", info_panel_rect, border_radius=10)  # Rounded corners
    pygame.draw.rect(screen, "black", info_panel_rect, width=2, border_radius=10)  # Border

    # Render title with bold font and color
    title_font = pygame.font.SysFont('arial', max(28, screen.get_width() // 50), bold=True)  # Adjust font size dynamically
    title_surface = title_font.render("*** EASY CAB Release 1 ***", True, "darkblue")
    screen.blit(title_surface, (info_x, 60))
    info_y = 140  # Adjust starting y position for better spacing

    # Render taxi information
    font_size = max(24, screen.get_width() // 60)  # Adjust font size dynamically
    dynamic_font = pygame.font.SysFont('arial', font_size)
    screen.blit(dynamic_font.render("Taxis:", True, "darkgreen"), (info_x, info_y))
    info_y += 60  # Increase spacing between sections
    for taxi in taxis:
        taxi_text = f"Id: {taxi[0]} Destino: {taxi[4]} Estado: {taxi[1]}"
        screen.blit(dynamic_font.render(taxi_text, True, "black"), (info_x, info_y))
        info_y += 40  # Increase line spacing

    # Render customer information
    info_y += 40  # Leave more space after taxi info
    screen.blit(dynamic_font.render("Clientes:", True, "darkred"), (info_x, info_y))
    info_y += 60  # Increase spacing between sections
    for customer in customers:
        if customer[1] == 'terminado':
            customer_text_lines = [
                f"Id: {customer[0]} Destino: {customer[1]}",
                "Estado: Servicios terminados"
            ]
        else:
            customer_text_lines = [
                f"Id: {customer[0]} Destino: {customer[1]}",
                f"Estado: {'Sin Taxi' if customer[1] == '' else 'OK'}"
            ]
        for line in customer_text_lines:
            screen.blit(dynamic_font.render(line, True, "black"), (info_x, info_y))
            info_y += 40  # Adjust line spacing
        info_y += 40  # Increase line spacing
    # flip() the display to put your work on screen
    pygame.display.flip()

    clock.tick(60)

con.execute('drop table taxis')
pygame.quit()
con.close()
consumer.close()
