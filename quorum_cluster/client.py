#!/usr/bin/env python3
"""
Client interattivo per il lab quorum.

Il client apre una connessione TCP verso il coordinator e inoltra ogni riga
scritta dall'utente. I comandi supportati sono:

    PING
    STATUS
    SET <key> <value>
    GET <key>
    QUIT

Il client non conosce le repliche e non implementa logica di quorum: il suo
unico compito è mandare i comandi al coordinator e stampare la risposta.
"""

import argparse
import socket


def parse_args() -> argparse.Namespace:
    """
    Legge host e porta del coordinator dalla riga di comando.

    I valori di default puntano al coordinator locale usato nel lab
    (`127.0.0.1:6420`).
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6420)
    return parser.parse_args()


def main() -> None:
    """Avvia il loop interattivo del client."""
    args = parse_args()

    # La connessione resta aperta finché l'utente non invia QUIT o EOF. Questo
    # replica il comportamento del client originale del professore: una sessione
    # TCP, piu' comandi testuali, una risposta per ogni comando.
    with socket.create_connection((args.host, args.port)) as connection:
        connection_file = connection.makefile("rwb")
        print(f"Connected to kv store on {args.host}:{args.port}")

        while True:
            try:
                line = input("kv> ")
            except EOFError:
                # EOF viene trattato come una chiusura ordinata della sessione.
                line = "QUIT"
                print()

            # Ogni comando è una riga UTF-8 terminata da newline. Il newline e'
            # importante perche' il server usa readline() per capire quando il
            # comando è completo.
            connection_file.write((line + "\n").encode("utf-8"))
            connection_file.flush()
            response = connection_file.readline()
            if not response:
                print("Connection closed by server.")
                break
            print(response.decode("utf-8", errors="replace").rstrip("\n"))
            if line.strip().upper() == "QUIT":
                break


if __name__ == "__main__":
    main()
