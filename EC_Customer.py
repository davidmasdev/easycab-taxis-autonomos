import sys
import pygame
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import time


idCustomer = sys.argv[1] if len(sys.argv) > 1 else "a"
dataBroker = sys.argv[2]  if len(sys.argv) > 2 else "localhost:9092"

f = open("servicios.txt")
servicios = f.read().splitlines()

# pygame setup
pygame.init()
screen = pygame.display.set_mode((1280, 720), pygame.RESIZABLE)
pygame.display.set_caption("EasyCab | EC_Customer")
pygame.font.init()
clock = pygame.time.Clock()
font = pygame.font.SysFont('arial', 24)
running = True

# Kafka Producer con mejor manejo de errores
def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=f'{dataBroker}',
            retries=3,
            acks='all',
            request_timeout_ms=10000
        )
        return producer
    except KafkaError as e:
        print(f"Error creando producer: {e}")
        return None

producer = create_producer()
if producer is None:
    print("No se pudo crear el producer. Saliendo...")
    sys.exit(1)

nextService = True
numService = 0

# Función para enviar mensajes con confirmación
def send_message_with_confirmation(topic, message, retries=3):
    if producer is None:
        print("Producer no disponible")
        return False
    
    for attempt in range(retries):
        try:
            future = producer.send(topic, message.encode())
            # Esperar confirmación
            record_metadata = future.get(timeout=10)
            print(f"Mensaje enviado exitosamente a {topic}: {message}")
            return True
        except KafkaError as e:
            print(f"Error enviando mensaje (intento {attempt + 1}/{retries}): {e}")
            if attempt == retries - 1:
                print(f"Falló el envío después de {retries} intentos")
                return False
            time.sleep(1)  # Esperar antes de reintentar
    return False

# Enviar mensaje de creación de cliente con confirmación
if not send_message_with_confirmation('customerCentral', f'create customer {idCustomer}'):
    print("Error: No se pudo crear el cliente. Saliendo...")
    sys.exit(1)

# Kafka Consumer con mejor manejo de errores
def create_consumer():
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=f'{dataBroker}',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f'customer_{idCustomer}',
            consumer_timeout_ms=1000
        )
        consumer.subscribe(['centralCustomer'])
        return consumer
    except KafkaError as e:
        print(f"Error creando consumer: {e}")
        return None

consumer = create_consumer()
if consumer is None:
    print("No se pudo crear el consumer. Saliendo...")
    sys.exit(1)

# Initialize a list to store messages for display
display_messages = []

def render_messages(screen, font, display_messages):
    screen.fill("white")  # Clear the screen
    y_offset = 0
    for message in display_messages[-20:]:  # Show only the last 20 messages
        text_surface = font.render(message, False, (0, 0, 0))
        screen.blit(text_surface, (10, 10 + y_offset))
        y_offset += 30
    pygame.display.flip()  # Update the display

cityState = 'OK'

while running:
    # poll for events
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    # fill the screen with a color to wipe away anything from last frame
    screen.fill("white")

    try:
        msg = consumer.poll(timeout_ms=100)
    except KafkaError as e:
        print(f"Error en consumer: {e}")
        # Intentar recrear el consumer
        if consumer is not None:
            consumer.close()
        consumer = create_consumer()
        if consumer is None:
            print("No se pudo recrear el consumer. Saliendo...")
            break
        continue

    if msg:
        for tp, records in msg.items():
            for record in records:
                try:
                    message = record.value.decode()
                    messageSplit = message.split(' ')
                    if messageSplit[0] == 'servicio' and messageSplit[1] == 'terminado' and messageSplit[2] == str(idCustomer):
                        log_message = f"Servicio {messageSplit[3]} terminado"
                        print(log_message)
                        display_messages.append(log_message)
                        render_messages(screen, font, display_messages)  # Call the function to render messages

                        nextService = True
                        if numService == len(servicios):
                            log_message = "Todos los servicios han sido solicitados"
                            print(log_message)
                            display_messages.append(log_message)
                            render_messages(screen, font, display_messages)  # Call the function to render messages

                            send_message_with_confirmation('customerCentral', f"terminados {idCustomer} todos los servicios")
                            # pygame.quit()
                            nextService = False
                            break

                    elif messageSplit[0] == 'denegacion' and messageSplit[1] == str(idCustomer):
                        log_message = f"Servicio {messageSplit[2]} denegado"
                        print(log_message)
                        display_messages.append(log_message)
                        render_messages(screen, font, display_messages)  # Call the function to render messages

                        time.sleep(4)
                        nextService = True
                        numService -= 1

                    elif messageSplit[0] == 'aceptado' and messageSplit[1] == str(idCustomer):
                        log_message = f"Servicio {messageSplit[2]} aceptado"
                        print(log_message)
                        display_messages.append(log_message)
                        render_messages(screen, font, display_messages)  # Call the function to render messages
                    
                    elif messageSplit[0] == 'parar':
                        cityState = "KO"
                        log_message = f"Servicio detenidos. No se puede circular por la ciudad"
                        print(log_message)
                        display_messages.append(log_message)
                        render_messages(screen, font, display_messages) 

                    elif messageSplit[0] == 'reanudar':
                        cityState = "OK"
                        nextService = True
                        numService -= 1
                        log_message = f"Servicio reanudados. Ya se puede circular por la ciudad"
                        print(log_message)
                        display_messages.append(log_message)
                        render_messages(screen, font, display_messages) 
                except Exception as e:
                    print(f"Error procesando mensaje: {e}")
                    continue

    if nextService and cityState == 'OK':
        time.sleep(4)
        log_message = f"Servicio {servicios[numService]} solicitado"
        if send_message_with_confirmation('customerCentral', f"service {servicios[numService]} customer {idCustomer}"):
            print(log_message)
            display_messages.append(log_message)
            render_messages(screen, font, display_messages)  # Call the function to render messages
            nextService = False
            numService += 1
        else:
            print(f"Error: No se pudo solicitar el servicio {servicios[numService]}")

    # Render and display all messages on the screen
    render_messages(screen, font, display_messages)

    clock.tick(60)  # limits FPS to 60

if producer is not None:
    producer.close()  
if consumer is not None:
    consumer.close() 
pygame.quit()
