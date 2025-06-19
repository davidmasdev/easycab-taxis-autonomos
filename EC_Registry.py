from flask import Flask, request
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error
import sys

# Parámetros para MySQL
mysql_host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
mysql_database = sys.argv[2] if len(sys.argv) > 2 else "easycab"
mysql_user = sys.argv[3] if len(sys.argv) > 3 else "easycab_user"
mysql_password = sys.argv[4] if len(sys.argv) > 4 else "Easycab23!"

# Configuración de la base de datos
DB_CONFIG = {
    'host': mysql_host,
    'database': mysql_database,
    'user': mysql_user,
    'password': mysql_password,
    'port': 3306
}

app = Flask(__name__)

CORS(app)

def get_db_connection():
    """Crear conexión a la base de datos MySQL"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error conectando a MySQL: {e}")
        return None

@app.route("/register", methods=['POST'])
def register():
    data = request.get_json()
    taxi_id = data.get('id')
    
    connection = get_db_connection()
    if not connection:
        return {"message": "Database connection error"}, 500
    
    try:
        cursor = connection.cursor()
        
        # Verificar si el taxi existe
        cursor.execute('SELECT estado FROM taxis WHERE id = %s', (taxi_id,))
        row = cursor.fetchone()
        
        if row:
            if row[0] == 'Sin autentificar':
                cursor.execute('UPDATE taxis SET estado = %s WHERE id = %s', ('OK', taxi_id))
                connection.commit()
                return {"message": "Taxi authenticated", "id": taxi_id}, 200
            else:
                return {"message": "Taxi already registered", "id": taxi_id}, 200
        else:
            cursor.execute(
                'INSERT INTO taxis (id, estado, posX, posY, destino) VALUES (%s, %s, %s, %s, %s)',
                (taxi_id, 'OK', 0, 0, '')
            )
            connection.commit()
            return {"message": "Taxi registered", "id": taxi_id}, 201
            
    except Error as e:
        print(f"Error en register: {e}")
        return {"message": "Database error"}, 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route("/cancel", methods=['DELETE'])
def cancel():
    data = request.get_json()
    taxi_id = data.get('id')
    
    connection = get_db_connection()
    if not connection:
        return {"message": "Database connection error"}, 500
    
    try:
        cursor = connection.cursor()
        
        # Verificar si el taxi existe
        cursor.execute('SELECT id FROM taxis WHERE id = %s', (taxi_id,))
        row = cursor.fetchone()
        
        if not row:
            return {"message": "Taxi not found", "id": taxi_id}, 404
        
        cursor.execute('DELETE FROM taxis WHERE id = %s', (taxi_id,))
        connection.commit()
        return {"message": "Taxi deleted", "id": taxi_id}, 200
        
    except Error as e:
        print(f"Error en cancel: {e}")
        return {"message": "Database error"}, 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=1234, debug=False, use_reloader=False)