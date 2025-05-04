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

ipEcs = ipPortEcs.split(":")[0]
portEcs = int(ipPortEcs.split(":")[1])

# Connect to Central
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((ipCentral, portCentral))
    s.sendall(f"{idTaxi}".encode())
    data = s.recv(1024)
    print(f"Received {data.decode()} from Central")

# Socket server creation
soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
soc.bind(('localhost', 4000))
soc.listen(1)
soc.setblocking(0)
conn = None
# conn, addr = soc.accept()
# conn.setblocking(0)

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

# Variable para almacenar el estado recibido de EC_S
estado_ec_s = "Esperando estado..."

while running:
    # pygame.QUIT event means the user clicked X to close your window
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    # fill the screen with a color to wipe away anything from last frame
    screen.fill("grey")

    # Get data from socket connection EC_S
    if conn is None:
        try:
            conn, addr = soc.accept()
            conn.setblocking(False)
            print("Cliente conectado")
        except BlockingIOError:
            pass  # No hay conexión entrante todavía
    else:
        try:
            data = conn.recv(1024).decode()
            if not data:
                break
            # print(f"Received {data} from EC_S")
            print (f"Recibido {data.strip()} de EC_S")
            if data.strip() != estado_ec_s[:2]:
                if estado.startswith('OK') or estado.startswith('KO'):
                    estado_ec_s = data.strip() + estado[2:]
                    print(f"Estado EC_S: {estado_ec_s}")
                else:
                    estado_ec_s = data.strip()
                producer.send('taxiCentral', (f'cambio {idTaxi} {customer} {estado_ec_s}').encode())
                print(f"Enviando: {estado_ec_s} a taxiCentral")
            # conn.close()
        except BlockingIOError:
            pass

    # Render the estado_ec_s on the screen
    estado_text = font.render(f"Estado EC_S: {estado_ec_s}", True, "white")

    # Calcula la posición para mostrar el texto en el lado derecho
    estado_x = screen.get_width() - estado_text.get_width() - 50  # 50 píxeles de margen desde el borde derecho
    estado_y = 50  # Mantén la posición vertical en 50 píxeles

    screen.blit(estado_text, (estado_x, estado_y))  # Muestra el texto en la nueva posición

    msg = consumer.poll(timeout_ms=100)

    if msg:
        for tp, records in msg.items():
            for record in records:
                message = record.value.decode()
                messageSplit = message.split(" ")
                if(message.strip().startswith('grid')):
                    grid = eval(message.strip()[5:])
                if(message.strip().startswith('taxis')):
                    taxis = eval(message.strip()[5:])
                    for taxiData in taxis:
                        if str(taxiData[0]) == idTaxi:
                            position = [taxiData[2],taxiData[3]]
                            estado = taxiData[1]
                if(message.strip().startswith('customers')):
                    customers = eval(message.strip()[9:])
                    for customerData in customers:
                        if customerData[0] == customer:
                            customerPosition = customerData[2]
                if(message.strip().startswith('locations')):
                    locations = eval(message.strip()[9:])
                if(messageSplit[0] == 'asignar' and messageSplit[1] == str(idTaxi)):
                    customer = messageSplit[3]
                    for customerData in customers:
                        if customerData[0] == customer:
                            customerPosition = customerData[2]
                    destination = messageSplit[5]
                    for location in locations:
                        if location[0] == destination:
                            locationPosition = location[1]
                            destinationPosition = [int(item) for item in locationPosition]
                    nextService = True

    # print(f"position {position} customerPosition {customerPosition} destinationPosition {destinationPosition}")
    if position != customerPosition and position != [] and customerPosition != [] and estado.startswith('OK'):
        # print(f"Taxi {idTaxi} recogiendo a {customer}")
        if position[0] < customerPosition[0] and position[1] < customerPosition[1]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0]+1, position[1]+1]}').encode())
        elif position[0] < customerPosition[0] and position[1] > customerPosition[1]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0]+1, position[1]-1]}').encode())
        elif position[0] > customerPosition[0] and position[1] < customerPosition[1]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0]-1, position[1]+1]}').encode())
        elif position[0] > customerPosition[0] and position[1] > customerPosition[1]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0]-1, position[1]-1]}').encode())
        elif position[0] < customerPosition[0]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0]+1, position[1]]}').encode())
        elif position[0] > customerPosition[0]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0]-1, position[1]]}').encode())
        elif position[1] < customerPosition[1]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0], position[1]+1]}').encode())
        elif position[1] > customerPosition[1]:
            producer.send('taxiCentral', (f'mover {idTaxi} {[position[0], position[1]-1]}').encode())

    if position == customerPosition and position != destinationPosition and estado.startswith('OK'):
        # print(f"Taxi {idTaxi} llevando a {customer} hacia {destination}")
        if position[0] < destinationPosition[0] and position[1] < destinationPosition[1]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0]+1, position[1]+1]}').encode())
        elif position[0] < destinationPosition[0] and position[1] > destinationPosition[1]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0]+1, position[1]-1]}').encode())
        elif position[0] > destinationPosition[0] and position[1] < destinationPosition[1]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0]-1, position[1]+1]}').encode())
        elif position[0] > destinationPosition[0] and position[1] > destinationPosition[1]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0]-1, position[1]-1]}').encode())
        elif position[0] < destinationPosition[0]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0]+1, position[1]]}').encode())
        elif position[0] > destinationPosition[0]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0]-1, position[1]]}').encode())
        elif position[1] < destinationPosition[1]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0], position[1]+1]}').encode())
        elif position[1] > destinationPosition[1]:
            producer.send('taxiCentral', (f'mover juntos {customer} {idTaxi} {[position[0], position[1]-1]}').encode())

    if position == destinationPosition and position != [] and position == customerPosition and estado.startswith('OK'):
        producer.send('taxiCentral', (f'terminado {customer} {destination} {idTaxi}').encode())
        nextService = False
        customer = ''
        destination = ''
        customerPosition = []
        destinationPosition = []
        # print(f"Taxi {idTaxi} dejando a {customer} en {destination}")

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
                    for taxi in taxis:
                        if taxi[0] == grid[row][column]:
                            if taxi[1].startswith('OK'):
                                color = "green"
                            else:
                                color = "red"
                elif (grid[row][column].islower()):
                    if len(grid[row][column]) > 1:
                        for taxi in taxis:
                            if str(taxi[0]) == grid[row][column][0]:
                                if taxi[1].startswith('OK'):
                                    color = "green"
                                else:
                                    color = "red"
                    else:
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

    clock.tick(1)


conn.close()    
consumer.close()
pygame.quit()

