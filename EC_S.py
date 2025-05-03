import sys
import pygame
import socket

ipPortEcde = sys.argv[1]

# pygame setup
pygame.init()
screen = pygame.display.set_mode((1280, 720), pygame.RESIZABLE)
pygame.display.set_caption("EasyCab | EC_Central")
clock = pygame.time.Clock()
font = pygame.font.SysFont('arial', 24)
running = True

# Socket creation
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect to the server
server_ip, server_port = ipPortEcde.split(":")
try:
    s.connect((server_ip, int(server_port)))
    print(f"Conectado al servidor en {server_ip}:{server_port}")
except ConnectionRefusedError:
    print("No se pudo conectar al servidor")
    sys.exit(1)

# Default message to send
message = b'OK\n'

# Game loop
while running:
    # poll for events
    # pygame.QUIT event means the user clicked X to close your window
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_s:  # Press 's' to send 'KO'
                message = b'KO\n'
            elif event.key == pygame.K_o:  # Press 'o' to send 'OK'
                message = b'OK\n'

    # fill the screen with a color to wipe away anything from last frame
    screen.fill("grey")
    
    # Render the current state on the screen
    state_text = font.render(f"Estado actual: {message.decode().strip()}", True, "white")
    screen.blit(state_text, (50, 50))  # Display the text at position (50, 50)

    # Send the current message every second
    try:
        s.sendall(message)
        print(f"Enviando: {message.decode().strip()}")
    except (BrokenPipeError, ConnectionResetError):
        print("Conexión perdida")
        running = False  # Exit the loop if the connection is lost

    pygame.display.flip()

    clock.tick(1)

# Close the socket when the game loop ends
s.close()