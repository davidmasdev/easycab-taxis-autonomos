import sys
import socket
from kafka import KafkaConsumer, KafkaProducer
import pygame
import requests
import secrets
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import base64

idTaxi = sys.argv[1] if len(sys.argv) > 1 else "1"
ipPortCentral = sys.argv[2] if len(sys.argv) > 2 else "localhost:3030"
ipPortBroker = sys.argv[3] if len(sys.argv) > 3 else "localhost:9092"
# ipPortEcs = sys.argv[4] if len(sys.argv) > 4 else "4040"

ipCentral = ipPortCentral.split(":")[0]
portCentral = int(ipPortCentral.split(":")[1])

# ipEcs = ipPortEcs.split(":")[0]
# portEcs = int(ipPortEcs.split(":")[1])

# Request to the EC_Registry api
print("Registrando en EC_Registry...")
try:
    response = requests.post("http://localhost:1234/register", 
                           json={'id': str(idTaxi)},
                           timeout=5)  # Timeout de 5 segundos
    
    if response.status_code == 200:
        print(f"✓ Registration successful: {response.text}")
    else:
        print(f"✗ Registration failed: {response.status_code} {response.text}")
        
except requests.exceptions.ConnectionError:
    print("✗ Error: No se puede conectar al EC_Registry (http://localhost:1234)")
except requests.exceptions.Timeout:
    print("✗ Error: Timeout conectando al EC_Registry")
except requests.exceptions.RequestException as e:
    print(f"✗ Network error: {e}")

# Connect to Central
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((ipCentral, portCentral))
    s.sendall(f"{idTaxi}".encode())
    data = s.recv(1024)
    response = data.decode()
    if response.startswith("Taxi autenticado|"):
        parts = response.split('|')
        token = parts[1]
        key_b64 = parts[2] if len(parts) > 2 else None
        if not key_b64:
            print("No se recibió clave AES de la central. Abortando.")
            sys.exit(1)
        key = base64.b64decode(key_b64)
        print(f"Received token and AES key from Central: {token}")
    else:
        print(f"Received unexpected response from Central: {response}")
        sys.exit(1)

# Socket server creation (bind to a free port in range 6000-7000)
soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
for port_try in range(6000, 7001):
    try:
        soc.bind(('localhost', port_try))
        port = port_try
        print(f"EC_DE listening on port {port}")
        break
    except OSError:
        continue
else:
    print("No free port found in range 6000-7000")
    sys.exit(1)

soc.listen(1)
soc.setblocking(False)
conn = None
# conn, addr = soc.accept()
# conn.setblocking(0)



# Kafka Producer
producer = KafkaProducer(bootstrap_servers=f'{ipPortBroker}')

# Kafka Consumer
consumer = KafkaConsumer(bootstrap_servers=f'{ipPortBroker}')
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
returnBase = False
basePosition = [0,0]

# Variable para almacenar el estado recibido de EC_S
estado_ec_s = "Esperando estado..."

# Funciones utilitarias de cifrado/descifrado

def encrypt_message_aes_gcm(key, plaintext):
    aesgcm = AESGCM(key)
    nonce = secrets.token_bytes(12)
    ct = aesgcm.encrypt(nonce, plaintext.encode(), None)
    return base64.b64encode(nonce + ct).decode()

def decrypt_message_aes_gcm(key, b64cipher):
    try:
        data = base64.b64decode(b64cipher)
        nonce, ct = data[:12], data[12:]
        aesgcm = AESGCM(key)
        pt = aesgcm.decrypt(nonce, ct, None)
        return pt.decode()
    except Exception as e:
        print(f"EC_DE Error descifrando mensaje: {e}")
        return None

estado = ""

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
                # Cifrar mensaje antes de enviarlo a taxiCentral
                encrypted_msg = encrypt_message_aes_gcm(key, f'cambio {idTaxi} {customer} {estado_ec_s}')
                producer.send('taxiCentral', encrypted_msg.encode())
                print(f"Enviando (cifrado): {estado_ec_s} a taxiCentral")
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
                b64cipher = record.value.decode()
                message = decrypt_message_aes_gcm(key, b64cipher)
                if not message:
                    continue
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
                if(messageSplit[0] == 'volver'):
                    returnBase = True
                    customer = ''
                    customerPosition = []
                    destination = []
                    destinationPosition = []
                if(messageSplit[0] == 'reanudar'):
                    returnBase = False

    if returnBase:
        if position[0] < basePosition[0] and position[1] < basePosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]+1, position[1]+1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] < basePosition[0] and position[1] > basePosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]+1, position[1]-1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] > basePosition[0] and position[1] < basePosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]-1, position[1]+1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] > basePosition[0] and position[1] > basePosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]-1, position[1]-1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] < basePosition[0]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]+1, position[1]]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] > basePosition[0]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]-1, position[1]]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[1] < basePosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0], position[1]+1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[1] > basePosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0], position[1]-1]}')
            producer.send('taxiCentral', encrypted_msg.encode())


    # print(f"position {position} customerPosition {customerPosition} destinationPosition {destinationPosition}")
    if not returnBase and position != customerPosition and position != [] and customerPosition != [] and estado.startswith('OK'):
        # print(f"Taxi {idTaxi} recogiendo a {customer}")
        if position[0] < customerPosition[0] and position[1] < customerPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]+1, position[1]+1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] < customerPosition[0] and position[1] > customerPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]+1, position[1]-1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] > customerPosition[0] and position[1] < customerPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]-1, position[1]+1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] > customerPosition[0] and position[1] > customerPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]-1, position[1]-1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] < customerPosition[0]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]+1, position[1]]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] > customerPosition[0]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0]-1, position[1]]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[1] < customerPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0], position[1]+1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[1] > customerPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover {idTaxi} {[position[0], position[1]-1]}')
            producer.send('taxiCentral', encrypted_msg.encode())

    if not returnBase and position == customerPosition and position != destinationPosition and estado.startswith('OK'):
        # print(f"Taxi {idTaxi} llevando a {customer} hacia {destination}")
        if position[0] < destinationPosition[0] and position[1] < destinationPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover juntos {customer} {idTaxi} {[position[0]+1, position[1]+1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] < destinationPosition[0] and position[1] > destinationPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover juntos {customer} {idTaxi} {[position[0]+1, position[1]-1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] > destinationPosition[0] and position[1] < destinationPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover juntos {customer} {idTaxi} {[position[0]-1, position[1]+1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] > destinationPosition[0] and position[1] > destinationPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover juntos {customer} {idTaxi} {[position[0]-1, position[1]-1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] < destinationPosition[0]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover juntos {customer} {idTaxi} {[position[0]+1, position[1]]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[0] > destinationPosition[0]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover juntos {customer} {idTaxi} {[position[0]-1, position[1]]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[1] < destinationPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover juntos {customer} {idTaxi} {[position[0], position[1]+1]}')
            producer.send('taxiCentral', encrypted_msg.encode())
        elif position[1] > destinationPosition[1]:
            encrypted_msg = encrypt_message_aes_gcm(key, f'mover juntos {customer} {idTaxi} {[position[0], position[1]-1]}')
            producer.send('taxiCentral', encrypted_msg.encode())

    if position == destinationPosition and position != [] and position == customerPosition and estado.startswith('OK'):
        encrypted_msg = encrypt_message_aes_gcm(key, f'terminado {customer} {destination} {idTaxi}')
        producer.send('taxiCentral', encrypted_msg.encode())
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


if conn is not None:
    conn.close()
consumer.close()
producer.close()  
pygame.quit()

