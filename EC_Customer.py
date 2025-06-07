import sys
import pygame
from kafka import KafkaProducer, KafkaConsumer
import time

dataBroker = sys.argv[1]
idCustomer = sys.argv[2]

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

# Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
nextService = True
numService = 0

producer.send('customerCentral', (f'create customer {idCustomer}').encode())

# Kafka Consumer
consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
consumer.subscribe(['centralCustomer'])

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

while running:
    # poll for events
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    # fill the screen with a color to wipe away anything from last frame
    screen.fill("white")

    msg = consumer.poll(timeout_ms=100)

    if msg:
        for tp, records in msg.items():
            for record in records:
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

                        producer.send('customerCentral', (f"terminados {idCustomer} todos los servicios").encode())
                        # pygame.quit()
                        nextService = False
                        break

                if messageSplit[0] == 'denegacion' and messageSplit[1] == str(idCustomer):
                    log_message = f"Servicio {messageSplit[2]} denegado"
                    print(log_message)
                    display_messages.append(log_message)
                    render_messages(screen, font, display_messages)  # Call the function to render messages

                    time.sleep(4)
                    nextService = True
                    numService -= 1

                if messageSplit[0] == 'aceptado' and messageSplit[1] == str(idCustomer):
                    log_message = f"Servicio {messageSplit[2]} aceptado"
                    print(log_message)
                    display_messages.append(log_message)
                    render_messages(screen, font, display_messages)  # Call the function to render messages

                    # time.sleep(5)
                    # nextService = True
                    # numService -= 1

    if nextService:
        time.sleep(4)
        log_message = f"Servicio {servicios[numService]} solicitado"
        producer.send('customerCentral', (f"service {servicios[numService]} customer {idCustomer}").encode())
        print(log_message)
        display_messages.append(log_message)
        render_messages(screen, font, display_messages)  # Call the function to render messages
        nextService = False
        numService += 1

    # Render and display all messages on the screen
    render_messages(screen, font, display_messages)

    clock.tick(60)  # limits FPS to 60

pygame.quit()
