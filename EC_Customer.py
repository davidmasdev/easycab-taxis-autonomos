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
screen = pygame.display.set_mode((1280, 720))
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

while running:
    # poll for events
    # pygame.QUIT event means the user clicked X to close your window
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    if(numService==len(servicios)):
        print("Todos los servicios han sido solicitados")
        pygame.quit()
        break

    # fill the screen with a color to wipe away anything from last frame
    screen.fill("white")

    text_surface = font.render('Hola Mundo', False, (0, 0, 0))

    screen.blit(text_surface, (0, 0))

    msg = consumer.poll(timeout_ms=100)

    if msg:
        for tp, records in msg.items():
            for record in records:
                # print(record)
                message = record.value.decode()
                messageSplit = record.value.decode().split(' ')
                if(messageSplit[0] == 'servicio' and messageSplit[1] == 'terminado' and messageSplit[2] == str(idCustomer)):
                    print(f"Servicio {messageSplit[3]} terminado")
                    nextService = True
                if(messageSplit[0] == 'denegacion' and messageSplit[1] == str(idCustomer)):
                    print(f"Servicio {messageSplit[2]} denegado")
                    time.sleep(5)
                    nextService = True
                    numService -= 1
                    
    if(nextService):
        time.sleep(4)
        producer.send('customerCentral', (f"service {servicios[numService]} customer {idCustomer}").encode())
        print(f"Servicio {servicios[numService]} solicitado")
        numService += 1
        nextService = False
    

    # flip() the display to put your work on screen
    pygame.display.flip()

    clock.tick(1)  # limits FPS to 60

pygame.quit()
