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
screen = pygame.display.set_mode((1280, 720), pygame.RESIZABLE)
pygame.display.set_caption(f'EasyCab | EC_DE Taxi {idTaxi}')
clock = pygame.time.Clock()
font = pygame.font.SysFont('arial', 24)
running = True

grid = []
customer = ''
customerPosition = []
destination = []
destinationPosition = []
position = []

nextService = False

while running:
    # pygame.QUIT event means the user clicked X to close your window
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    # fill the screen with a color to wipe away anything from last frame
    screen.fill("grey")

    msg = consumer.poll(timeout_ms=100)

    if msg:
        for tp, records in msg.items():
            for record in records:
                message = record.value.decode()
                messageSplit = message.split(" ")
                if(message.strip().startswith('grid')):
                    grid = eval(message.strip()[5:])
                if(messageSplit[0] == 'asignar' and messageSplit[1] == str(idTaxi)):
                    customer = messageSplit[3]
                    print(customer)
                    destination = messageSplit[5]
                    print(destination)
                    nextService = True
                   
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

    if grid != []:
        for row in range(20):
            for column in range(20):
                rect = pygame.Rect([(MARGIN + WIDTH) * column + MARGIN + x_offset,
                                (MARGIN + HEIGHT) * row + MARGIN + y_offset,
                                WIDTH,
                                HEIGHT])

                color = "white"
                if (grid[row][column] != 0):
                    if (isinstance(grid[row][column], int)):
                        if (grid[row][column] == int(idTaxi)):
                            position = [row, column]
                        color = "green"
                    elif grid[row][column][0] == str(idTaxi) and grid[row][column][1] == customer:
                        position = [row, column]
                        customerPosition = [row, column]
                        color = "green"
                    elif (grid[row][column].islower()):
                        if (grid[row][column] == customer):
                            customerPosition = [row, column]
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

    print(f"position {position} customerPosition {customerPosition} destinationPosition {destinationPosition}")
    if position != customerPosition and position != [] and customerPosition != []:
        print(f"Taxi {idTaxi} recogiendo a {customer}")
        if position[0] < customerPosition[0]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0]+1, position[1]]}').encode())
        elif position[0] > customerPosition[0]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0]-1, position[1]]}').encode())
        elif position[1] < customerPosition[1]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0], position[1]+1]}').encode())
        elif position[1] > customerPosition[1]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0], position[1]-1]}').encode()) 

    if position == customerPosition and position != destinationPosition:
        print(f"Taxi {idTaxi} llevando a {customer} hacia {destination}")
        if position[0] < destinationPosition[0]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0]+1, position[1]]}').encode())
        elif position[0] > destinationPosition[0]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0]-1, position[1]]}').encode())
        elif position[1] < destinationPosition[1]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0], position[1]+1]}').encode())
        elif position[1] > destinationPosition[1]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0], position[1]-1]}').encode())

    if position == destinationPosition and position != [] and position == customerPosition:
        producer.send('taxiCentral', (f'terminado {customer} {destination} {idTaxi}').encode())
        nextService = False
        customer = ''
        destination = ''
        customerPosition = []
        destinationPosition = []
        print(f"Taxi {idTaxi} dejando a {customer} en {destination}")

    # flip() the display to put
    #  your work on screen
    pygame.display.flip()

    clock.tick(1)


consumer.close()
pygame.quit()

