from flask import Flask, jsonify
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app)

AUDIT_LOG_PATH = 'audit.log'

# Estructura esperada: 'YYYY-MM-DD HH:MM:SS | IP | ACCION | DESCRIPCION'
def parse_audit_line(line):
    try:
        parts = line.strip().split(' | ')
        if len(parts) != 4:
            return None
        return {
            'timestamp': parts[0],
            'ip': parts[1],
            'action': parts[2],
            'description': parts[3]
        }
    except Exception:
        return None

@app.route('/audit', methods=['GET'])
def get_audit():
    if not os.path.exists(AUDIT_LOG_PATH):
        return jsonify({'error': 'No audit log found'}), 404
    events = []
    with open(AUDIT_LOG_PATH, 'r') as f:
        for line in f:
            event = parse_audit_line(line)
            if event:
                events.append(event)
    return jsonify({'events': events})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081, debug=False) 