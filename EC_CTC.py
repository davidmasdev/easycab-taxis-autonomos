import requests
from flask import Flask

app = Flask(__name__)
apiKey = "7f49d719db92256be845cf81ab1a59f6"

city = "Madrid"
state = "Ok"

def updateState():
    response = requests.get("http://api.openweathermap.org/data/2.5/weather?q={city}&appid={apiKey}")
    datos = response.json()
    print(datos)
    if datos["main"]["temp"] < 0:
        state = "KO"
    else:
        state = "OK"

app.get("/estado")
def estado():
    updateState()
    return state

run = True

while(run):
    opt = input("Elige la ciudad ('salir' para cerrar): ")
    if opt == "salir":
        exit()
    else:
        city = opt
        print(f'Nueva ciudad: {city}')

app.run(host='0.0.0.0', port=5000)
