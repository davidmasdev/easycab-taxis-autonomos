import requests
import threading
import time
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Configuración
API_KEY = "7f49d719db92256be845cf81ab1a59f6"
city = "Madrid"
state = "OK"
server_running = False

def update_state(silent=False):
    """Actualiza el estado basado en la temperatura de la ciudad actual"""
    global state
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        datos = response.json()
        if not silent:
            print(f"Datos recibidos para {city}: {datos}")
        
        if "main" in datos and "temp" in datos["main"]:
            temp = datos["main"]["temp"]
            if not silent:
                print(f"Temperatura en {city}: {temp}°C")
            
            if temp < 0:
                state = "KO"
            else:
                state = "OK"
        else:
            if not silent:
                print("Error: No se encontraron datos de temperatura")
            state = "ERROR"
            
    except requests.exceptions.RequestException as e:
        if not silent:
            print(f"Error en la petición: {e}")
        state = "ERROR"
    except KeyError as e:
        if not silent:
            print(f"Error en los datos recibidos: {e}")
        state = "ERROR"
    except Exception as e:
        if not silent:
            print(f"Error inesperado: {e}")
        state = "ERROR"

@app.route("/estado")
def estado():
    """Endpoint que devuelve el estado actual"""
    update_state(silent=True)  # Modo silencioso para evitar spam en consola
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

@app.route("/cambiar_ciudad", methods=['POST'])
def cambiar_ciudad_post():
    """Endpoint POST para cambiar la ciudad"""
    global city
    data = request.get_json()
    if not data or 'ciudad' not in data:
        return jsonify({"error": "Se requiere el campo 'ciudad'"}), 400
    
    city = data['ciudad']
    print(f'Nueva ciudad: {city}')
    update_state()
    return jsonify({
        "mensaje": f"Ciudad cambiada a {city}",
        "estado": state
    })

@app.route("/info")
def info():
    """Endpoint que muestra información sobre el servidor"""
    return jsonify({
        "servidor": "Servidor Meteorológico Flask",
        "ciudad_actual": city,
        "estado_actual": state,
        "endpoints": {
            "GET /estado": "Obtiene el estado actual",
            "GET /cambiar_ciudad/<ciudad>": "Cambia la ciudad",
            "POST /cambiar_ciudad": "Cambia la ciudad (JSON: {'ciudad': 'nombre'})",
            "GET /info": "Muestra esta información"
        }
    })

def console_interface():
    """Interfaz de consola que se ejecuta en paralelo al servidor"""
    global city, server_running
    
    print("\n=== CONSOLA INTERACTIVA ===")
    print("El servidor Flask está funcionando en segundo plano")
    print("Comandos disponibles:")
    print("  - Escribe el nombre de una ciudad para cambiarla")
    print("  - 'estado' para ver el estado actual")
    print("  - 'info' para ver información del servidor")
    print("  - 'salir' para cerrar todo")
    print()
    
    while server_running:
        try:
            opt = input("Comando: ").strip()
            
            if opt.lower() == "salir":
                print("Cerrando servidor...")
                server_running = False
                break
                
            elif opt.lower() == "estado":
                update_state()
                print(f"Ciudad: {city}")
                print(f"Estado: {state}")
                
            elif opt.lower() == "info":
                print(f"Servidor ejecutándose en: http://localhost:5050")
                print(f"Ciudad actual: {city}")
                print(f"Estado actual: {state}")
                print("Endpoints disponibles:")
                print("  - GET /estado")
                print("  - GET /cambiar_ciudad/<ciudad>")
                print("  - POST /cambiar_ciudad")
                print("  - GET /info")
                
            elif opt != "":
                city = opt
                print(f'Nueva ciudad: {city}')
                update_state()
                print(f"Estado actualizado: {state}")
                
        except KeyboardInterrupt:
            print("\nCerrando servidor...")
            server_running = False
            break
        except Exception as e:
            print(f"Error: {e}")

def run_flask_server():
    """Ejecuta el servidor Flask"""
    import logging
    # Desabilitar logs de Flask para requests automáticos
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    
    try:
        app.run(host='0.0.0.0', port=5050, debug=False, use_reloader=False)
    except Exception as e:
        print(f"Error en el servidor: {e}")

def main():
    """Función principal"""
    global server_running
    
    print("=== SERVIDOR METEOROLÓGICO CON CONSOLA INTERACTIVA ===")
    
    # Servidor con consola interactiva
    server_running = True
    
    # Iniciar el servidor Flask en un hilo separado
    server_thread = threading.Thread(target=run_flask_server, daemon=True)
    server_thread.start()
    
    # Esperar un poco para que el servidor se inicie
    time.sleep(2)
    print(f"\nServidor Flask iniciado en http://localhost:5050")
    print("Puedes usar los endpoints web Y la consola interactiva")
    
    # Ejecutar la consola interactiva en el hilo principal
    console_interface()

if __name__ == "__main__":
    main()