import sys
import pygame
from kafka import KafkaProducer
import base64


dataBroker = sys.argv[1]
idCustomer = sys.argv[2]

f = open("servicios.txt")
servicios = f.read()

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

while running:
    # poll for events
    # pygame.QUIT event means the user clicked X to close your window
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    # fill the screen with a color to wipe away anything from last frame
    screen.fill("white")

    text_surface = font.render('Hola Mundo', False, (0, 0, 0))

    screen.blit(text_surface, (0, 0))

    if(nextService):
        if(numService==0):
            producer.send('customerCentral', (f'create customer {idCustomer}').encode())
        producer.send('customerCentral', (f"service {servicios[numService]} customer {idCustomer}").encode())
        nextService = False
        i=+1

    # flip() the display to put your work on screen
    pygame.display.flip()

    clock.tick(60)  # limits FPS to 60

pygame.quit()
