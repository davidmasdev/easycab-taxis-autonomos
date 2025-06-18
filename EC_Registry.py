from flask import Flask, request
from flask_cors import CORS
import sqlite3
import sys

database = sys.argv[1] if len(sys.argv) > 1 else "database.db"

app = Flask(__name__)

CORS(app)

@app.route("/register", methods=['POST'])
def register():
    data = request.get_json()
    taxi_id = data.get('id')
    con = sqlite3.connect(database, check_same_thread=False)
    cur = con.cursor()
    cur.execute('SELECT estado FROM taxis WHERE id = ?', (taxi_id,))
    row = cur.fetchone()
    if row:
        if row[0] == 'Sin autentificar':
            cur.execute('UPDATE taxis SET estado = ? WHERE id = ?', ('OK', taxi_id))
            con.commit()
            con.close()
            return {"message": "Taxi authenticated", "id": taxi_id}, 200
        else:
            con.close()
            return {"message": "Taxi already registered", "id": taxi_id}, 200
    else:
        cur.execute(
            'INSERT INTO taxis (id, estado, posX, posY, destino) VALUES (?, ?, ?, ?, ?)',
            (taxi_id, 'OK', 0, 0, '')
        )
        con.commit()
        con.close()
        return {"message": "Taxi registered", "id": taxi_id}, 201
    

@app.route("/cancel", methods=['DELETE'])
def cancel():
    data = request.get_json()
    taxi_id = data.get('id')
    con = sqlite3.connect(database, check_same_thread=False)
    cur = con.cursor()
    cur.execute('SELECT id FROM taxis WHERE id = ?', (taxi_id,))
    row = cur.fetchone()
    if not row:
        con.close()
        return {"message": "Taxi not found", "id": taxi_id}, 404
    cur.execute('DELETE FROM taxis WHERE id = ?', (taxi_id,))
    con.commit()
    con.close()
    return {"message": "Taxi deleted", "id": taxi_id}, 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=1234, debug=False, use_reloader=False)