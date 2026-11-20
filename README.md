# EasyCab: autonomous taxi fleet

A real-time simulation of a fleet of autonomous taxis on a 20×20 city map. Seven Python services talk to each other
over Kafka, TCP sockets and Flask REST APIs: customers request rides, the central server assigns taxis and tracks
them, and two React dashboards show the live map and the audit log.

**Python · Kafka · TCP sockets · Flask · MySQL · AES-256-GCM · React · Docker Compose**

![EasyCab map](https://github.com/user-attachments/assets/5faec367-b213-4c3a-b7a4-f4bd9d90a7b8)

## Architecture

| Service | Role | Talks over |
|---|---|---|
| `EC_Central.py` | Central server. Authenticates taxis, assigns rides, keeps the taxis' state in MySQL and serves the map to the dashboard. | TCP socket (3030), Kafka, REST (7070) |
| `EC_DE.py` | A taxi ("digital engine"). Authenticates with the central server, then drives to the customer and to the destination step by step. | TCP socket, Kafka |
| `EC_S.py` | A taxi's sensor. Reports `OK`, or `KO` when there is an obstacle. | TCP socket to its taxi |
| `EC_Customer.py` | A customer. Requests the rides listed in `servicios.txt`, one after another. | Kafka |
| `EC_Registry.py` | Registers and removes taxis in the database. | REST (1234) |
| `EC_CTC.py` | City traffic control. Checks a city's temperature with the OpenWeatherMap API; below 0 °C the central server stops all rides and sends the taxis back to base. | REST (5050) |
| `EC_AuditAPI.py` | Serves the audit log to the audit dashboard. | REST (7071) |

`frontend/` is a React dashboard with the live map (port 8080), and `frontend-audit/` shows the audit log (port 8081).

## Security

- When a taxi authenticates over the socket, the central server gives it a session token and its own AES-256-GCM
  key. Every message between that taxi and the central server over Kafka is encrypted with that key and a random
  12-byte nonce.
- Messages with a token that is not the current one are rejected. Tokens are revoked when a taxi is sent back to
  base, so it has to authenticate again.
- Relevant events (authentication, state changes, rides created and finished) go to an audit log with their
  timestamp and source IP.

## Running it

You need Docker, Python 3 and an [OpenWeatherMap](https://openweathermap.org/api) API key.

```bash
cp .env.example .env            # add your OpenWeatherMap key
./start-deamons.sh              # detects your IP and starts Kafka, MySQL, the audit API and both dashboards
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python EC_Registry.py &
python EC_CTC.py &
./start_improved.sh             # central server, 2 taxis with their sensors and 3 customers
```

`start_improved.sh` waits for each service to be reachable before starting the next one. Every service also takes its
addresses as command-line arguments, so the components can run on different machines, for example
`python EC_DE.py <taxi id> <central host:port> <kafka host:port>`.

The map locations are read from `mapa.txt` (one per line: `ID|x|y`) and each customer's rides from `servicios.txt`.

## What was hard

- Services starting in the wrong order, solved with a startup script that waits for each port.
- Kafka addressing: the broker advertises different listeners for the host, for other containers and for other
  machines on the network.
- Keeping the state of many taxis and customers consistent in the central server while it reads from Kafka, the
  sockets and the REST API at the same time.
