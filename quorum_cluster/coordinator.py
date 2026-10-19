#!/usr/bin/env python3
"""
Coordinator con quorum di lettura/scrittura e read repair.

Il coordinator riceve comandi testuali dai client e parla con le repliche usando
messaggi JSON su TCP. Per ogni chiave:

- SET assegna una nuova versione intera monotona rispetto alle repliche lette;
- GET legge almeno R repliche e restituisce la versione piu' recente osservata;
- dopo GET, il read repair aggiorna le repliche stantie viste nella lettura.

Questa è la parte centrale di Homework 2: il client non decide nulla sulle
versioni, mentre le repliche salvano soltanto i dati ricevuti. Il coordinator è
quindi il componente che applica il contratto di quorum, sceglie la versione
corretta e avvia il repair.

Le RPC verso le repliche sono lanciate in parallelo: appena arrivano R risposte
valide (o W ACK per SET) la fase termina e il client riceve la risposta. Cosi'
una replica lenta non costringe il coordinator ad aspettare i timeout in
sequenza, come richiesto dalla proprietà L2 della traccia ("una replica lenta
non deve impedire ogni lettura se il quorum minimo è raggiungibile").
"""

import argparse
import json
import socket
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

CommandHandler = Callable[[str], tuple[str, bool]]
ReadResult = tuple["ReplicaEndpoint", dict[str, object]]


class ConflictError(Exception):
    """
    Errore usato quando stessa versione e valori diversi sono osservati.

    La traccia chiede di vietare o trattare come conflitto questo caso. Qui il
    coordinator restituisce `ERR conflict` invece di scegliere un valore a caso.
    """


def log(message: str) -> None:
    """Stampa log con timestamp e nome del thread che gestisce la richiesta."""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    thread_name = threading.current_thread().name
    print(f"[{timestamp}] [{thread_name}] {message}")


@dataclass(frozen=True)
class ReplicaEndpoint:
    """Indirizzo TCP di una replica del quorum store."""

    host: str
    port: int


def parse_args() -> argparse.Namespace:
    """
    Configura porta del coordinator, quorum R/W e lista delle repliche.

    N è dato dal numero di endpoint passati con `--replicas`. Con i default del
    lab si hanno N=3, R=2 e W=2.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6420)
    parser.add_argument("--read-quorum", type=int, default=2)
    parser.add_argument("--write-quorum", type=int, default=2)
    parser.add_argument(
        "--replicas",
        nargs="+",
        default=["127.0.0.1:6421", "127.0.0.1:6422", "127.0.0.1:6423"],
    )
    return parser.parse_args()


class QuorumCoordinator:
    """
    Implementa i comandi del KV store con quorum e read repair.

    `read_quorum` e `write_quorum` sono i valori R e W della traccia. Una GET
    deve ricevere almeno R risposte valide; una SET deve ricevere almeno W ACK
    dalle repliche.
    """

    def __init__(self, replicas: list[ReplicaEndpoint], read_quorum: int, write_quorum: int) -> None:
        self._replicas = replicas
        self._read_quorum = read_quorum
        self._write_quorum = write_quorum
        # Lock per chiave: serializza le SET concorrenti sulla stessa chiave
        # cosi' la sequenza "leggi max versione + scrivi max+1" risulta atomica
        # dal punto di vista del coordinator. Senza questo lock due SET
        # simultanei sulla stessa chiave potrebbero scegliere lo stesso
        # next_version e produrre un conflitto persistente sulle repliche.
        self._key_locks_mutex = threading.Lock()
        self._key_locks: dict[str, threading.Lock] = {}
        self._handlers: dict[str, CommandHandler] = {
            "PING": self._handle_ping,
            "STATUS": self._handle_status,
            "SET": self._handle_set,
            "GET": self._handle_get,
            "QUIT": self._handle_quit,
        }

    def execute(self, line: str) -> tuple[str, bool]:
        """
        Esegue una riga di comando ricevuta dal client.

        Ritorna una coppia `(risposta, should_close)`: `should_close` diventa
        True solo con QUIT, cosi' il thread client sa quando chiudere la
        connessione.
        """
        stripped = line.strip()
        if not stripped:
            return "ERR empty command", False
        command, *rest = stripped.split(" ", 1)
        command = command.upper()
        argument_blob = rest[0] if rest else ""
        handler = self._handlers.get(command)
        if handler is None:
            return "ERR unknown command", False
        return handler(argument_blob)

    def _lock_for(self, key: str) -> threading.Lock:
        """Restituisce (creandolo se serve) il lock dedicato a una chiave."""
        with self._key_locks_mutex:
            lock = self._key_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._key_locks[key] = lock
            return lock

    def _rpc(self, replica: ReplicaEndpoint, message: dict[str, object]) -> dict[str, object] | None:
        """
        Invia un messaggio JSON a una replica e restituisce la risposta.

        Se la replica è spenta, lenta o risponde con JSON non valido, ritorna
        None. Questo permette al coordinator di ignorare una replica guasta e
        continuare se il quorum minimo è comunque raggiungibile.
        """
        try:
            with socket.create_connection((replica.host, replica.port), timeout=1.0) as connection:
                connection_file = connection.makefile("rwb")
                payload = json.dumps(message) + "\n"
                connection_file.write(payload.encode("utf-8"))
                connection_file.flush()
                response = connection_file.readline().decode("utf-8", errors="replace").strip()
        except OSError:
            return None
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            return None

    def _collect_reads(self, key: str, limit: int | None = None) -> list[ReadResult]:
        """
        Legge la chiave dalle repliche raggiungibili fino al limite indicato.

        Le RPC partono in parallelo verso tutte le repliche e le risposte
        valide vengono raccolte nell'ordine di arrivo. Per GET si passa
        `limit=R` e si interrompe la raccolta non appena si hanno R risposte
        valide, cosi' una replica lenta non blocca la lettura (L2).

        Per SET non viene passato un limite, cosi' il coordinator osserva tutte
        le repliche raggiungibili prima di scegliere la prossima versione.
        """
        results: list[ReadResult] = []
        if not self._replicas:
            return results

        pool = ThreadPoolExecutor(max_workers=len(self._replicas))
        try:
            futures = {
                pool.submit(self._rpc, replica, {"type": "read", "key": key}): replica
                for replica in self._replicas
            }
            for future in as_completed(futures):
                response = future.result()
                if response is None or response.get("status") != "OK":
                    continue
                results.append((futures[future], response))
                if limit is not None and len(results) >= limit:
                    break
        finally:
            # Non aspettiamo i thread ancora in volo: una replica lenta puo'
            # restare in background, ma il client ha gia' la risposta.
            pool.shutdown(wait=False)
        return results

    def _highest_version(self, reads: list[ReadResult]) -> tuple[int, str | None]:
        """
        Sceglie la versione piu' recente tra le risposte valide.

        Se due repliche espongono la stessa versione ma valori diversi, il
        coordinator non puo' scegliere in modo sicuro e segnala conflitto.

        Le risposte `found=False` non partecipano alla scelta del massimo:
        indicano solo che quella replica non ha ancora la chiave e potrà
        eventualmente essere riparata dal read repair.
        """
        best_version = -1
        best_value: str | None = None
        values_by_version: dict[int, str] = {}
        for _, read in reads:
            if not bool(read.get("found", False)):
                continue
            version = int(read.get("version", -1))
            value = str(read.get("value", ""))
            if version in values_by_version and values_by_version[version] != value:
                raise ConflictError(f"ERR conflict version={version}")
            values_by_version[version] = value
            if version > best_version:
                best_version = version
                best_value = value
        return best_version, best_value

    def _read_repair(self, key: str, reads: list[ReadResult], version: int, value: str) -> int:
        """
        Aggiorna le repliche lette che non hanno la versione piu' recente.

        Il repair viene fatto solo sulle repliche osservate nella GET. Se una
        replica non ha risposto, non blocca la lettura e potrà essere riparata
        da una GET successiva quando tornerà raggiungibile.

        Le scritture di repair partono in parallelo sulle sole repliche stale.
        La replica protegge comunque il dato locale: un repair tardivo che
        portasse una versione vecchia verrebbe rifiutato (proprietà S2).
        """
        targets = [replica for replica, read in reads if self._is_stale(read, version)]
        if not targets:
            return 0

        repairs = 0
        pool = ThreadPoolExecutor(max_workers=len(targets))
        try:
            futures = [
                pool.submit(
                    self._rpc,
                    replica,
                    {"type": "write", "key": key, "value": value, "version": version},
                )
                for replica in targets
            ]
            for future in as_completed(futures):
                response = future.result()
                if response is not None and response.get("status") == "ACK":
                    repairs += 1
        finally:
            pool.shutdown(wait=False)
        return repairs

    def _is_stale(self, read: dict[str, object], version: int) -> bool:
        """
        Ritorna True se una risposta è assente o ha una versione vecchia.

        Una replica che risponde `found=False` è stale rispetto a un valore
        trovato su un'altra replica del quorum.
        """
        if not bool(read.get("found", False)):
            return True
        return int(read.get("version", -1)) < version

    def _handle_ping(self, argument_blob: str) -> tuple[str, bool]:
        """Risponde al comando PING usato per verificare che il server sia vivo."""
        if argument_blob.strip():
            return "ERR usage: PING", False
        return "OK PONG", False

    def _handle_status(self, argument_blob: str) -> tuple[str, bool]:
        """Restituisce N, R e W configurati nel coordinator."""
        if argument_blob.strip():
            return "ERR usage: STATUS", False
        return (
            f"OK N={len(self._replicas)} R={self._read_quorum} W={self._write_quorum}",
            False,
        )

    def _handle_set(self, argument_blob: str) -> tuple[str, bool]:
        """
        Scrive un valore su almeno W repliche assegnando una nuova versione.

        La versione è decisa dal coordinator: legge le versioni esistenti,
        prende la massima e scrive `massima + 1`. Questo soddisfa l'hint della
        traccia quando si usano versioni intere locali.

        L'intera sequenza "leggi max + scrivi max+1" viene serializzata da un
        lock per chiave, cosi' due SET concorrenti sulla stessa chiave non
        possono produrre due scritture con la stessa nuova versione e valori
        diversi (anomalia che porterebbe a un conflitto persistente).

        Le scritture partono in parallelo verso tutte le repliche e il
        comando risponde OK appena raggiunge W ACK.
        """
        parts = argument_blob.split(" ", 1)
        if len(parts) != 2 or not parts[0]:
            return "ERR usage: SET <key> <value>", False
        key, value = parts

        with self._lock_for(key):
            reads = self._collect_reads(key)
            try:
                current_version, _ = self._highest_version(reads)
            except ConflictError as exc:
                return str(exc), False

            # Versioni intere locali: il coordinator prende la massima
            # osservata e assegna il successivo intero al nuovo valore.
            next_version = current_version + 1

            acknowledgements = 0
            pool = ThreadPoolExecutor(max_workers=len(self._replicas))
            try:
                futures = [
                    pool.submit(
                        self._rpc,
                        replica,
                        {
                            "type": "write",
                            "key": key,
                            "value": value,
                            "version": next_version,
                        },
                    )
                    for replica in self._replicas
                ]
                for future in as_completed(futures):
                    response = future.result()
                    if response is not None and response.get("status") == "ACK":
                        acknowledgements += 1
                        if acknowledgements >= self._write_quorum:
                            return (
                                f"OK version={next_version} acks={acknowledgements}",
                                False,
                            )
            finally:
                pool.shutdown(wait=False)

            return f"ERR write quorum not reached acks={acknowledgements}", False

    def _handle_get(self, argument_blob: str) -> tuple[str, bool]:
        """
        Legge da almeno R repliche, sceglie la versione massima e ripara.

        Questa funzione implementa il requisito principale della homework:
        una lettura non deve restituire una versione vecchia se nel quorum letto
        ne esiste una piu' recente.
        """
        key = argument_blob.strip()
        if not key:
            return "ERR usage: GET <key>", False

        reads = self._collect_reads(key, limit=self._read_quorum)
        if len(reads) < self._read_quorum:
            return f"ERR read quorum not reached responses={len(reads)}", False

        try:
            version, value = self._highest_version(reads)
        except ConflictError as exc:
            return str(exc), False
        if version < 0 or value is None:
            return "NOT_FOUND", False

        # Read repair: dopo aver scelto la versione piu' recente, il coordinator
        # riscrive quella versione sulle repliche del quorum che erano rimaste
        # indietro. La replica protegge comunque il dato locale da repair vecchi.
        repairs = self._read_repair(key, reads, version, value)
        if repairs:
            log(f"read repair key={key} version={version} repairs={repairs}")
        return f"OK {value} version={version}", False

    def _handle_quit(self, argument_blob: str) -> tuple[str, bool]:
        """Chiude in modo ordinato la connessione con il client."""
        if argument_blob.strip():
            return "ERR usage: QUIT", False
        return "OK BYE", True


def handle_client(
        connection: socket.socket,
        address: tuple[str, int],
        coordinator: QuorumCoordinator,
) -> None:
    """
    Gestisce una connessione client leggendo comandi riga per riga.

    Ogni client viene servito da un thread separato, cosi' piu' client possono
    parlare con lo stesso coordinator contemporaneamente.
    """
    log(f"client connection from {address[0]}:{address[1]}")
    with connection:
        connection_file = connection.makefile("rwb")
        while True:
            raw_line = connection_file.readline()
            if not raw_line:
                break
            line = raw_line.decode("utf-8", errors="replace")
            log(f"request: {line.rstrip()}")
            response, should_close = coordinator.execute(line)
            connection_file.write((response + "\n").encode("utf-8"))
            connection_file.flush()
            log(f"response: {response}")
            if should_close:
                break


def serve() -> None:
    """
    Avvia il server TCP del coordinator e crea un thread per ogni client.

    La lista `--replicas` viene convertita da stringhe `host:port` in endpoint
    usati dalle RPC interne verso i replica node.
    """
    args = parse_args()
    replicas = [
        ReplicaEndpoint(host=entry.split(":", 1)[0], port=int(entry.split(":", 1)[1]))
        for entry in args.replicas
    ]
    coordinator = QuorumCoordinator(replicas, args.read_quorum, args.write_quorum)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((args.host, args.port))
        server_socket.listen()
        log(
            f"quorum coordinator listening on {args.host}:{args.port} "
            f"N={len(replicas)} R={args.read_quorum} W={args.write_quorum}"
        )

        while True:
            connection, address = server_socket.accept()
            threading.Thread(
                target=handle_client,
                args=(connection, address, coordinator),
                daemon=True,
            ).start()


if __name__ == "__main__":
    serve()
