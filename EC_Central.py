import pygame
import sys
import sqlite3
from kafka import KafkaConsumer, KafkaProducer
import random
import socket
import requests
from flask import Flask, jsonify
from flask_cors import CORS
import threading
import time

# Variables globales para compartir datos entre hilos
taxis = []
customers = []
locations = []
grid = []

app = Flask(__name__)
CORS(app)

@app.route("/central")
def central():
    global taxis, customers, locations, grid, cityState
    return jsonify({
        "grid": grid,
        "taxis": taxis,
        "customers": customers,
        "locations": locations,
        "cityState": cityState
    })

def run_flask_app():
    """Función para ejecutar Flask en un hilo separado"""
    app.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)

def main():
    global taxis, customers, locations, grid, cityState
    
    # Params
    listenPort = sys.argv[1] if len(sys.argv) > 1 else "3030"
    dataBroker = sys.argv[2] if len(sys.argv) > 2 else "localhost:9092"
    database = sys.argv[3] if len(sys.argv) > 3 else "database.db"
    locationParam = sys.argv[4] if len(sys.argv) > 4 else "mapa.txt"

    # Iniciar Flask en un hilo separado
    flask_thread = threading.Thread(target=run_flask_app, daemon=True)
    flask_thread.start()
    print("Flask API iniciada en http://localhost:8080")

    # Locations treatment
    try:
        f = open(locationParam)
        rawLocations = f.read()
        unformatedLocations = rawLocations.split()
        locations = []
        for location in unformatedLocations:
            newLocation = location.split("|")
            locations.append([newLocation[0], [newLocation[1], newLocation[2]]])
        f.close()
    except FileNotFoundError:
        print(f"Archivo de ubicaciones {locationParam} no encontrado. Usando ubicaciones por defecto.")
        locations = [
            ['A', ['5', '5']],
            ['B', ['10', '10']],
            ['C', ['15', '15']]
        ]

    # Database treatment
    con = sqlite3.connect(database, check_same_thread=False)
    cur = con.cursor()
    
    # Crear tabla si no existe
    cur.execute('CREATE TABLE IF NOT EXISTS taxis (id INTEGER PRIMARY KEY AUTOINCREMENT, estado TEXT, posX INTEGER, posY INTEGER, destino TEXT)')
    
    # Verificar si ya hay datos en la tabla
    cur.execute('SELECT COUNT(*) FROM taxis')
    count = cur.fetchone()[0]
    
    if count == 0:
        # Solo insertar si la tabla está vacía
        cur.execute('INSERT INTO taxis (estado, posX, posY, destino) VALUES (?, ?, ?, ?)', ('Sin autenticar', 0, 0, ''))
        cur.execute('INSERT INTO taxis (estado, posX, posY, destino) VALUES (?, ?, ?, ?)', ('Sin autenticar', 0, 0, ''))
        cur.execute('INSERT INTO taxis (estado, posX, posY, destino) VALUES (?, ?, ?, ?)', ('Sin autenticar', 0, 0, ''))
        cur.execute('INSERT INTO taxis (estado, posX, posY, destino) VALUES (?, ?, ?, ?)', ('Sin autenticar', 0, 0, ''))
        con.commit()

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
    try:
        producer = KafkaProducer(bootstrap_servers=f'{dataBroker}')
        producer.send('taxiCentral', (f'').encode())
        print("Kafka Producer conectado")
    except Exception as e:
        print(f"Error conectando Kafka Producer: {e}")
        producer = None

    # Kafka Consumer
    try:
        consumer = KafkaConsumer(bootstrap_servers=f'{dataBroker}')
        consumer.subscribe(['customerCentral', 'taxiCentral'])
        print("Kafka Consumer conectado")
    except Exception as e:
        print(f"Error conectando Kafka Consumer: {e}")
        consumer = None

    customers = []

    # Socket creation
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('127.0.0.1', int(listenPort)))
        s.listen(1)
        s.setblocking(0)
        print(f"Socket servidor iniciado en puerto {listenPort}")
    except Exception as e:
        print(f"Error creando socket: {e}")
        s = None

    lastApiCall = 0 
    cityState = 'OK'

    # Game loop
    while running:
        # poll for events
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_p:  # Press 'P' to stop a taxi
                    try:
                        taxi_id = int(input("Ingrese el ID del taxi a detener: "))
                        cur.execute('UPDATE taxis SET estado = ? WHERE id = ?', ('KO', taxi_id))
                        con.commit()
                        print(f"Taxi {taxi_id} ha sido detenido.")
                    except ValueError:
                        print("ID inválido")
                elif event.key == pygame.K_r:  # Press 'R' to resume a taxi
                    try:
                        taxi_id = int(input("Ingrese el ID del taxi a reanudar: "))
                        cur.execute('UPDATE taxis SET estado = ? WHERE id = ?', ('OK', taxi_id))
                        con.commit()
                        print(f"Taxi {taxi_id} ha reanudado el servicio.")
                    except ValueError:
                        print("ID inválido")
                elif event.key == pygame.K_d:  # Press 'D' to send a taxi to a destination
                    try:
                        taxi_id = int(input("Ingrese el ID del taxi: "))
                        new_destination = input("Ingrese el nuevo destino (Letra mayúscula): ")
                        cur.execute('UPDATE taxis SET destino = ? WHERE id = ?', (str(new_destination), taxi_id))
                        con.commit()
                        print(f"Taxi {taxi_id} ahora se dirige a {new_destination}.")
                    except ValueError:
                        print("ID inválido")
                elif event.key == pygame.K_b:  # Press 'B' to send a taxi back to base
                    try:
                        taxi_id = int(input("Ingrese el ID del taxi a enviar a la base: "))
                        cur.execute('UPDATE taxis SET destino = ? WHERE id = ?', ('[0,0]', taxi_id))
                        con.commit()
                        print(f"Taxi {taxi_id} está volviendo a la base.")
                    except ValueError:
                        print("ID inválido")

        # fill the screen with a color to wipe away anything from last frame
        screen.fill("grey")

        # CTC api request
        changeState = False
        if time.time() - lastApiCall >= 10:
            response = requests.get("http://localhost:5050/estado")
            if response.ok:
                data = response.json()
                if cityState != data['estado']:
                    changeState = True
                cityState = data['estado']

        # Check if cityState is KO and make taxis go to base
        if changeState and cityState == "KO":
            for taxi in taxis:
                if taxi[1] != "Sin autenticar":
                    producer.send('centralTaxi', (f'volver base').encode())
                    cur.execute('UPDATE taxis SET destino = ?, estado = ? WHERE id = ?', ('', 'OK. Volver a base', taxi[0]))
            producer.send('centralCustomer', (f'parar servicios').encode())
            for customer in customers:
                customer[1] = ''
        elif changeState and cityState == "OK":
                for taxi in taxis:
                    if taxi[1] != "Sin autenticar":
                        cur.execute('UPDATE taxis SET destino = ?, estado = ? WHERE id = ?', ('', 'OK', taxi[0]))
                producer.send('centralCustomer', (f'reanudar servicios').encode())
                producer.send('centralTaxi', (f'reanudar servicios').encode())


        # Get taxis from db
        try:
            res = cur.execute("SELECT * FROM taxis")
            taxis = res.fetchall()
        except Exception as e:
            print(f"Error accediendo a la base de datos: {e}")
            taxis = []

        # Get taxis from socket connection
        if s:
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
                            cur.execute('UPDATE taxis SET estado = ? WHERE id = ?', ('Esperando estado...', taxi[0]))
                            con.commit()
                            conn.sendall(("Taxi autenticado").encode())
                conn.close()
            except BlockingIOError:
                pass
            except Exception as e:
                print(f"Error en socket: {e}")

        # Grid treatment
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

        # Kafka message processing
        if consumer:
            try:
                msg = consumer.poll(timeout_ms=100)
                if msg:
                    for tp, records in msg.items():
                        for record in records:
                            message = record.value.decode()
                            messageSplit = record.value.decode().split(' ')
                            
                            if(messageSplit[0] == 'create'):
                                posX = random.choice([x for x in range(20) if x not in [int(subarray[1][0]) for subarray in locations]])
                                posY = random.choice([y for y in range(20) if y not in [int(subarray[1][1]) for subarray in locations]])
                                customers.append([messageSplit[2], '', [posX, posY]])
                                grid[posX][posY] = messageSplit[2]
                                print(f'Creado customer {messageSplit[2]} en posición {[posX, posY]}')

                            if(messageSplit[0] == 'service'):
                                for customer in customers:
                                    if customer[0] == messageSplit[3]:
                                        asigned = False
                                        customer[1] = messageSplit[1]
                                        for taxi in taxis:
                                            print(taxi)
                                            if(taxi[4] == '' and taxi[1] == 'OK'):
                                                cur.execute('UPDATE taxis SET destino = ?, estado = ? WHERE id = ?', (customer[1], f'OK. Servicio {customer[0]}', taxi[0]))
                                                con.commit()
                                                if producer:
                                                    producer.send('centralTaxi', (f'asignar {taxi[0]} cliente {customer[0]} destino {customer[1]}').encode())
                                                    producer.send('centralCustomer', (f'aceptado {customer[0]} {customer[1]}').encode())
                                                print(f'Customer {customer[0]} con servicio a {customer[1]} asignado al taxi {taxi[0]}')
                                                asigned = True
                                                break
                                        if not asigned and producer:
                                            producer.send('centralCustomer', (f'denegacion {customer[0]} {messageSplit[1]} de servicio').encode())

                            # Resto del procesamiento de mensajes Kafka...
                            if(messageSplit[0] == 'mover'):
                                for taxi in taxis:
                                    if messageSplit[1] == 'juntos' and str(taxi[0]) == messageSplit[3]:
                                        newPos = eval(message[message.find('['):message.find(']')+1])
                                        print(newPos[0], newPos[1])
                                        cur.execute('UPDATE taxis SET posX = ?, posY = ? WHERE id = ?', (newPos[0], newPos[1], taxi[0]))
                                        con.commit()
                                        for customer in customers:
                                            if customer[0] == messageSplit[2]:
                                                customer[2] = [newPos[0], newPos[1]]

                                    elif str(taxi[0]) == messageSplit[1]:
                                        newPos = eval(message[message.find('['):message.find(']')+1])
                                        cur.execute('UPDATE taxis SET posX = ?, posY = ? WHERE id = ?', (newPos[0], newPos[1], taxi[0]))
                                        con.commit()
                            
                            if(messageSplit[0] == 'terminado'):
                                if producer:
                                    producer.send('centralCustomer', (f'servicio terminado {messageSplit[1]} {messageSplit[2]}').encode())
                                cur.execute('UPDATE taxis SET estado = ?, destino = ? WHERE id = ?', ('OK', '', messageSplit[3]))
                                con.commit()
                                for customer in customers:
                                    if customer[0] == messageSplit[1]:
                                        customer[1] = ''
                                        print(f'Servicio {messageSplit[1]} terminado')
                                        
                            if(messageSplit[0] == 'terminados'):
                                for customer in customers:
                                    if customer[0] == messageSplit[1]:
                                        customer[1] = 'terminado'
                                        print(f'Customer {messageSplit[0]} ha terminado todos los servicios')
                                        
                            if(messageSplit[0] == 'cambio'):
                                for taxi in taxis:
                                    if str(taxi[0]) == messageSplit[1]:
                                        new_state = ' '.join(messageSplit[3:])
                                        cur.execute('UPDATE taxis SET estado = ? WHERE id = ?', (new_state, taxi[0]))
                                        con.commit()
                                        print(f'Taxi {messageSplit[1]} ha cambiado su estado a {new_state}')
            except Exception as e:
                print(f"Error procesando mensajes Kafka: {e}")

        # Send updates via Kafka
        if producer:
            try:
                producer.send('centralTaxi', (f'grid {str(grid)}').encode())
                producer.send('centralTaxi', (f'taxis {str(taxis)}').encode())
                producer.send('centralTaxi', (f'customers {str(customers)}').encode())
                producer.send('centralTaxi', (f'locations {str(locations)}').encode())
            except Exception as e:
                print(f"Error enviando mensajes Kafka: {e}")
        
        # RENDER GAME
        grid_width = 20 * (WIDTH + MARGIN) + MARGIN
        grid_height = 20 * (HEIGHT + MARGIN) + MARGIN

        # Calculate offsets for centering in y and adding margin in x
        x_offset = 50
        grid_width = screen.get_width() - 100
        grid_height = screen.get_height() - 100

        cell_width = (grid_width - (MARGIN * 21)) // 20
        cell_height = (grid_height - (MARGIN * 21)) // 20

        cell_size = min(cell_width, cell_height)
        WIDTH = HEIGHT = max(20, cell_size)

        grid_width = 20 * (WIDTH + MARGIN) + MARGIN
        grid_height = 20 * (HEIGHT + MARGIN) + MARGIN

        x_offset = 50
        y_offset = (screen.get_height() - grid_height) // 2

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
                    elif (str(grid[row][column]).islower()):
                        if len(str(grid[row][column])) > 1:
                            for taxi in taxis:
                                if str(taxi[0]) == str(grid[row][column])[0]:
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

        # Render taxi and customer information
        info_width = max(450, screen.get_width() // 3)
        info_x = grid_width + (screen.get_width() - grid_width - info_width) // 2
        info_panel_rect = pygame.Rect(info_x - 20, 50, info_width, screen.get_height() - 80)
        pygame.draw.rect(screen, "lightgrey", info_panel_rect, border_radius=10)
        pygame.draw.rect(screen, "black", info_panel_rect, width=2, border_radius=10)

        title_font = pygame.font.SysFont('arial', max(28, screen.get_width() // 50), bold=True)
        title_surface = title_font.render("*** EASY CAB Release 1 ***", True, "darkblue")
        screen.blit(title_surface, (info_x, 60))
        info_y = 140

        font_size = max(24, screen.get_width() // 60)
        dynamic_font = pygame.font.SysFont('arial', font_size)
        screen.blit(dynamic_font.render("Taxis:", True, "darkgreen"), (info_x, info_y))
        info_y += 60
        for taxi in taxis:
            taxi_text = f"Id: {taxi[0]} Destino: {taxi[4]} Estado: {taxi[1]}"
            screen.blit(dynamic_font.render(taxi_text, True, "black"), (info_x, info_y))
            info_y += 40

        info_y += 40
        screen.blit(dynamic_font.render("Clientes:", True, "darkred"), (info_x, info_y))
        info_y += 60
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
                info_y += 40
            info_y += 40

        # Render city state
        city_state_text = font.render(f"Estado de la ciudad: {cityState}", True, (255, 255, 255))  # Blanco
        screen.blit(city_state_text, (10, 10))  # Posición en la pantalla

        pygame.display.flip()
        clock.tick(60)

    # Cleanup
    pygame.quit()
    try:
        if con:
            # Reset the sequence BEFORE dropping the table
            cur.execute('DELETE FROM sqlite_sequence WHERE name="taxis"')
            cur.execute('DROP TABLE IF EXISTS taxis')
            con.commit()
            con.close()
    except Exception as e:
        print(f"Error cerrando la base de datos: {e}")
    try:
        if s:
            s.close()
    except Exception as e:
        print(f"Error cerrando el socket: {e}")
    try:
        if producer:
            producer.close()
    except Exception as e:
        print(f"Error cerrando Kafka Producer: {e}")
    try:
        if consumer:
            consumer.close()
    except Exception as e:
        print(f"Error cerrando Kafka Consumer: {e}")

if __name__ == "__main__":
    main()