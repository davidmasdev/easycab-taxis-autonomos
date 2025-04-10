import pygame
import sys


# Params
listenPort = sys.argv[1]
dataBroker = sys.argv[2]
portDB = sys.argv[3]
locationParam = sys.argv[4]

# Locations treatment
f = open(locationParam)
rawLocations = f.read()
unformatedLocations = rawLocations.split()
locations = []
for location in unformatedLocations:
    newLocation = location.split("|")
    locations.append([newLocation[0], [newLocation[1], newLocation[2]]])

grid = []
locationFound = False

for row in range(20):
    grid.append([])
    for column in range(20):
        locationFound = False
        for location in locations:
            if(location[1] == [str(row), str(column)]):
                grid[row].append(location[0])
                locationFound = True
        if(locationFound == False):
            grid[row].append(0)


# This sets the WIDTH and HEIGHT of each grid location
WIDTH = 20
HEIGHT = 20

# This sets the margin between each cell
MARGIN = 5

# pygame setup
pygame.init()
screen = pygame.display.set_mode((1280, 720))
pygame.display.set_caption("EasyCab | EC_Central")
clock = pygame.time.Clock()
font = pygame.font.SysFont('arial', 24)
running = True

while running:
    # poll for events
    # pygame.QUIT event means the user clicked X to close your window
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    # fill the screen with a color to wipe away anything from last frame
    screen.fill("black")

    # RENDER YOUR GAME HERE
    for row in range(20):
        for column in range(20):
            rect = pygame.Rect([(MARGIN + WIDTH) * column + MARGIN,
                                (MARGIN + HEIGHT) * row + MARGIN,
                                WIDTH,
                                HEIGHT])

            color = "white"
            if (grid[row][column] != 0):
                color = "blue"
            pygame.draw.rect(screen, color, rect)

            if (grid[row][column] != 0):
                text_surface = font.render(grid[row][column], True, "white")
                text_rect = text_surface.get_rect(center=rect.center)
                screen.blit(text_surface, text_rect)
    # flip() the display to put your work on screen
    pygame.display.flip()

    clock.tick(60)  # limits FPS to 60

pygame.quit()
