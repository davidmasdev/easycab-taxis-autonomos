import requests
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Configuración
API_KEY = "7f49d719db92256be845cf81ab1a59f6"
city = "Madrid"
state = "OK"

def update_state():
    """Actualiza el estado basado en la temperatura de la ciudad actual"""
    global state
    try:
        # Corregir la URL con f-string
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Lanza excepción si hay error HTTP
        
        datos = response.json()
        print(f"Datos recibidos para {city}: {datos}")
        
        # Verificar si la respuesta contiene los datos esperados
        if "main" in datos and "temp" in datos["main"]:
            temp = datos["main"]["temp"]
            print(f"Temperatura en {city}: {temp}°C")
            
            # Actualizar estado basado en temperatura
            if temp < 0:
                state = "KO"
            else:
                state = "OK"
        else:
            print("Error: No se encontraron datos de temperatura")
            state = "ERROR"
            
    except requests.exceptions.RequestException as e:
        print(f"Error en la petición: {e}")
        state = "ERROR"
    except KeyError as e:
        print(f"Error en los datos recibidos: {e}")
        state = "ERROR"
    except Exception as e:
        print(f"Error inesperado: {e}")
        state = "ERROR"

@app.route("/estado")
def estado():
    """Endpoint que devuelve el estado actual"""
    update_state()
    return jsonify({
        "ciudad": city,
        "estado": state
    })

@app.route("/cambiar_ciudad/<nueva_ciudad>")
def cambiar_ciudad(nueva_ciudad):
    """Endpoint para cambiar la ciudad"""
    global city
    city = nueva_ciudad
    print(f'Nueva ciudad: {city}')
    update_state()
    return jsonify({
        "mensaje": f"Ciudad cambiada a {city}",
        "estado": state
    })

def main():
    """Función principal que maneja la entrada del usuario"""
    global city
    
    print("=== SERVIDOR METEOROLÓGICO ===")
    print("Comandos disponibles:")
    print("  - Escribe el nombre de una ciudad para cambiarla")
    print("  - 'servidor' para iniciar el servidor web")
    print("  - 'estado' para ver el estado actual")
    print("  - 'salir' para cerrar")
    print()
    
    while True:
        try:
            opt = input("Comando: ").strip()
            
            if opt.lower() == "salir":
                print("¡Hasta luego!")
                break
                
            elif opt.lower() == "servidor":
                print("Iniciando servidor Flask...")
                print("Accede a http://localhost:5050/estado para ver el estado")
                print("Accede a http://localhost:5050/cambiar_ciudad/NombreCiudad para cambiar la ciudad")
                print("Presiona Ctrl+C para detener el servidor y volver al menú")
                try:
                    app.run(host='0.0.0.0', port=5050, debug=False)
                except KeyboardInterrupt:
                    print("\nServidor detenido. Volviendo al menú...")
                    
            elif opt.lower() == "estado":
                update_state()
                print(f"Ciudad: {city}")
                print(f"Estado: {state}")
                
            elif opt != "":
                city = opt
                print(f'Nueva ciudad: {city}')
                update_state()
                print(f"Estado actualizado: {state}")
                
        except KeyboardInterrupt:
            print("\n¡Hasta luego!")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()