#!/usr/bin/env python3
"""
Replica node per laboratorio quorum.

Ogni replica espone un piccolo protocollo JSON su TCP:

- read: restituisce value e version se la chiave esiste;
- write: salva value e version se la versione non e' piu' vecchia;
- status: restituisce le chiavi presenti nella replica.

La replica non decide quale valore sia globale: questa responsabilita' resta al
coordinator. La replica applica solo una regola locale di sicurezza: non deve
sovrascrivere una versione piu' nuova con una piu' vecchia.
"""

import argparse
import json
import socket
import threading
from datetime import datetime


def log(message: str) -> None:
    """Stampa log con timestamp e nome del thread che gestisce la connessione."""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    thread_name = threading.current_thread().name
    print(f"[{timestamp}] [{thread_name}] {message}")


def parse_args() -> argparse.Namespace:
    """
    Legge identificativo e indirizzo TCP della replica.

    `--node-id` serve nei log e nelle risposte JSON per capire quale replica ha
    risposto durante gli esperimenti del lab.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    return parser.parse_args()


class ReplicaNode:
    """
    Replica in memoria che salva ogni chiave come coppia value/version.

    `_data` non viene salvato su disco: riavviare il processo azzera lo stato.
    Il lock protegge il dizionario perche' ogni connessione viene gestita in un
    thread separato.
    """

    def __init__(self, node_id: str) -> None:
        self._node_id = node_id
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, object]] = {}

    def handle_message(self, raw_line: str) -> str:
        """
        Esegue un messaggio JSON ricevuto dal coordinator.

        Tutte le risposte sono JSON serializzati su una sola riga, cosi' il
        coordinator puo' usare `readline()` per riceverle.
        """
        try:
            message = json.loads(raw_line)
        except json.JSONDecodeError:
            return json.dumps({"status": "ERR", "error": "invalid json"})

        message_type = message.get("type")
        if message_type == "read":
            # Lettura: la replica restituisce anche la versione del valore. Il
            # coordinator usera' questa versione per scegliere il valore piu'
            # recente tra le repliche del quorum letto.
            key = str(message.get("key", ""))
            if not key:
                return json.dumps({"status": "ERR", "error": "missing key"})
            with self._lock:
                record = self._data.get(key)
            if record is None:
                return json.dumps(
                    {"status": "OK", "found": False, "node": self._node_id, "key": key}
                )
            return json.dumps(
                {
                    "status": "OK",
                    "found": True,
                    "node": self._node_id,
                    "key": key,
                    "value": record["value"],
                    "version": record["version"],
                }
            )

        if message_type == "write":
            # Scrittura: versioni vecchie non sovrascrivono dati piu' recenti.
            # Questa regola protegge anche il read repair: se nel frattempo la
            # replica ha gia' una versione piu' nuova, il repair vecchio non la
            # puo' cancellare.
            key = str(message.get("key", ""))
            value = str(message.get("value", ""))
            version = int(message.get("version", 0))
            if not key:
                return json.dumps({"status": "ERR", "error": "missing key"})
            with self._lock:
                current = self._data.get(key)
                current_version = int(current["version"]) if current else -1
                if (
                    current is not None
                    and version == current_version
                    and str(current["value"]) != value
                ):
                    # Stessa versione e valore diverso: conflitto esplicito.
                    # La traccia richiede di vietare o trattare come conflitto
                    # questo caso, perche' non esiste un criterio sicuro per
                    # scegliere uno dei due valori.
                    return json.dumps(
                        {
                            "status": "ERR",
                            "error": "conflict",
                            "node": self._node_id,
                            "version": version,
                        }
                    )
                if version >= current_version:
                    # Versione nuova o uguale con stesso valore: la scrittura e'
                    # accettata. Il caso "uguale con valore diverso" e' gia'
                    # stato bloccato sopra come conflitto.
                    self._data[key] = {"value": value, "version": version}
            return json.dumps({"status": "ACK", "node": self._node_id, "version": version})

        if message_type == "status":
            # STATUS serve per ispezionare lo stato della replica durante il lab.
            # Non fa parte della logica di quorum, ma aiuta a verificare quali
            # chiavi sono presenti su ogni nodo.
            with self._lock:
                keys = sorted(self._data.keys())
            return json.dumps({"status": "OK", "node": self._node_id, "keys": keys})

        return json.dumps({"status": "ERR", "error": "unknown type"})


def handle_connection(
    connection: socket.socket,
    address: tuple[str, int],
    replica: ReplicaNode,
) -> None:
    """
    Gestisce una connessione TCP verso una replica.

    Anche qui si usa un protocollo line-based: una richiesta JSON per riga e una
    risposta JSON per riga.
    """
    log(f"connection from {address[0]}:{address[1]}")
    with connection:
        connection_file = connection.makefile("rwb")
        while True:
            raw_line = connection_file.readline()
            if not raw_line:
                break
            response = replica.handle_message(raw_line.decode("utf-8", errors="replace").strip())
            connection_file.write((response + "\n").encode("utf-8"))
            connection_file.flush()
            log(f"response: {response}")


def serve() -> None:
    """
    Avvia la replica e crea un thread per ogni connessione entrante.

    Ogni replica e' un processo indipendente. Nel lab se ne avviano tre su porte
    diverse, poi il coordinator le contatta usando la lista `--replicas`.
    """
    args = parse_args()
    replica = ReplicaNode(args.node_id)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((args.host, args.port))
        server_socket.listen()
        log(f"replica {args.node_id} listening on {args.host}:{args.port}")

        while True:
            connection, address = server_socket.accept()
            threading.Thread(
                target=handle_connection,
                args=(connection, address, replica),
                daemon=True,
            ).start()


if __name__ == "__main__":
    serve()
