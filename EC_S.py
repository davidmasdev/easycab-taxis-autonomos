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
s.bind((ipPortEcde.split(":")[0], int(ipPortEcde.split(":")[1])))
s.listen(1)
s.setblocking(0)

# Accept a connection (non-blocking)
conn = None
try:
    conn, addr = s.accept()
    print(f"Conexión establecida con {addr}")
except BlockingIOError:
    pass

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

    # Send the current message every second if a connection exists
    if conn:
        try:
            conn.sendall(message)
        except (BrokenPipeError, ConnectionResetError):
            print("Conexión perdida")
            conn = None  # Reset connection if it's lost

    pygame.display.flip()

    clock.tick(1)