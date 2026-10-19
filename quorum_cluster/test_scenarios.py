#!/usr/bin/env python3
"""
Test suite automatico per Quorum Store con Read Repair.
============================================================

Questo file e' il cuore del lavoro del Fault/Test Owner.
Contiene 10 test che parlano con il sistema reale via TCP (niente mock).
Mappa test -> proprieta':
  Test 1  PING            -> smoke test (il coordinator e' vivo)
  Test 2  STATUS          -> configurazione N/R/W (precondizione di L2)
  Test 3  SET + GET       -> S1 implicita (la lettura vede la scrittura)
  Test 4  versioni        -> S1 (versioni monotone, ordinabili)
  Test 5  read repair     -> L1 (repliche stale convergono) + S2 (no regressioni)
  Test 6  NOT_FOUND       -> contratto pubblico
  Test 7  ERR usage       -> contratto pubblico (input malformato)
  Test 8  unknown command -> contratto pubblico (comando fuori protocollo)
  Test 9  QUIT            -> contratto pubblico (chiusura ordinata)
  Test 10 conflict        -> S3 (stessa versione + valori diversi -> ERR esplicito)

I due test "che contano" dal punto di vista distribuito sono il 5 e il 10:
- il 5 forza una replica vuota e verifica che il read repair la guarisca;
- il 10 forza una situazione di conflitto e verifica che il sistema avvisi
  il client invece di scegliere un valore arbitrario (filosofia fail-safe).

Per il test 5 e il 10 il driver bypassa il coordinator e parla direttamente
con le repliche in JSON:
serve a portare le repliche in uno stato controllato che il coordinator,
da solo, non saprebbe creare.

Uso:
    python test_scenarios.py [--host 127.0.0.1] [--port 6420]
                             [--replica-ports 6421 6422 6423]

Prerequisiti:
    - Coordinator in esecuzione su --host:--port
    - Tutte e 3 le repliche in esecuzione sulle porte indicate
"""

import argparse
import json
import socket
import sys
import time
from typing import Optional, Tuple

# Su Windows il code page di default (cp1252) non sa stampare i marker ✓/✗.
# Riconfiguriamo lo stdout in UTF-8 cosi' la suite gira anche da PowerShell o cmd.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, OSError):
    pass


# ----------------------------------------------------------------------------
# HELPER DI RETE
# ----------------------------------------------------------------------------
# Due funzioni soltanto: una per parlare con il coordinator (protocollo
# testuale "PING\n", "SET k v\n", ...) e una per parlare direttamente con
# una replica (protocollo JSON). Tutto il resto e' composizione di queste due.


def send_command(host: str, port: int, command: str) -> Optional[str]:
    """Invia un comando testuale al coordinator e ritorna la prima riga di risposta."""
    try:
        # Apriamo una connessione TCP nuova per ogni comando: e' piu' semplice
        # da ragionare nei test e isola gli errori di un test dagli altri.
        with socket.create_connection((host, port), timeout=2.0) as conn:
            conn_file = conn.makefile("rwb")
            conn_file.write((command + "\n").encode("utf-8"))
            conn_file.flush()
            # Il contratto pubblico garantisce una riga di risposta per ogni comando.
            response = conn_file.readline().decode("utf-8", errors="replace").strip()
            return response if response else None
    except (socket.timeout, OSError) as e:
        # Se il coordinator e' giu' o non risponde in tempo,
        # consideriamo il test fallito ma non rompiamo l'intera suite.
        print(f"    ✗ Connessione al coordinator fallita: {e}")
        return None


def send_to_replica(host: str, port: int, message: dict) -> Optional[dict]:
    """
    Invia un messaggio JSON direttamente a una replica, bypassando il coordinator.

    Serve ai test 5 (L1) e 10 (S3) per portare le repliche in uno stato
    controllato che il coordinator non saprebbe mai creare da solo:
    - test 5: scrivere su R1+R2 lasciando R3 vuota
    - test 10: scrivere la stessa versione con valori diversi su tutte le repliche

    Il protocollo JSON usato qui e' lo stesso che il coordinator usa
    internamente per le sue RPC verso le repliche.
    """
    try:
        with socket.create_connection((host, port), timeout=2.0) as conn:
            conn_file = conn.makefile("rwb")
            conn_file.write((json.dumps(message) + "\n").encode("utf-8"))
            conn_file.flush()
            raw = conn_file.readline().decode("utf-8", errors="replace").strip()
            if not raw:
                return None
            return json.loads(raw)
    except (socket.timeout, OSError, json.JSONDecodeError) as e:
        print(f"    ✗ Connessione alla replica {host}:{port} fallita: {e}")
        return None


# ----------------------------------------------------------------------------
# TEST 1 — PING
# ----------------------------------------------------------------------------
# Smoke test: se questo fallisce non ha senso fare gli altri.
# In demo apre il discorso: "prima controllo che il coordinator sia vivo".

def test_ping(host: str, port: int) -> bool:
    """Test 1: PING - il coordinator e' vivo."""
    print("\n[TEST 1] PING - Server alive check")
    resp = send_command(host, port, "PING")
    # Il contratto pubblico fissa la risposta esatta: "OK PONG".
    # Niente fuzzy matching: il test e' OK solo se ricevo esattamente quella stringa.
    expected = "OK PONG"
    if resp == expected:
        print(f"    ✓ {resp}")
        return True
    print(f"    ✗ Atteso '{expected}', ricevuto '{resp}'")
    return False


# ----------------------------------------------------------------------------
# TEST 2 — STATUS
# ----------------------------------------------------------------------------
# Verifica la configurazione del quorum: N, R, W.
# E' la precondizione di L2: per dire "una replica lenta non blocca" devo
# prima assicurarmi che R < N (altrimenti L2 non si puo' nemmeno enunciare).

def test_status(host: str, port: int) -> Tuple[bool, int, int]:
    """Test 2: STATUS - configurazione del quorum."""
    print("\n[TEST 2] STATUS - Configuration check")
    resp = send_command(host, port, "STATUS")
    # Formato atteso dal contratto: "OK N=<n> R=<r> W=<w>".
    if resp and resp.startswith("OK N="):
        print(f"    ✓ {resp}")
        # Parsing semplice ma robusto: split su spazi, poi su '='.
        parts = resp.split()
        n = int(parts[1].split("=")[1])
        r = int(parts[2].split("=")[1])
        w = int(parts[3].split("=")[1])
        print(f"    N={n}, R={r}, W={w}")
        # Ritorno R e W cosi' altri test potrebbero adattarsi alla config
        # se un giorno volessimo provare N=5, R=3, W=3.
        return True, r, w
    print(f"    ✗ Risposta inattesa: {resp}")
    return False, -1, -1


# ----------------------------------------------------------------------------
# TEST 3 — SET + GET NOMINALE
# ----------------------------------------------------------------------------
# Il caso felice: scrivo un valore e lo rileggo. E' una prima verifica
# intuitiva di S1: dopo una scrittura riuscita, una lettura non deve
# restituire un valore sbagliato.

def test_set_get_nominal(host: str, port: int) -> bool:
    """Test 3: SET + GET nominale (S1 implicito: GET ritorna la versione scritta)."""
    print("\n[TEST 3] SET + GET - Nominal case")
    # Chiave con timestamp per non collidere con altri test eseguiti di fila.
    key = f"nominal_{int(time.time())}"
    value = "test_value_1"

    resp_set = send_command(host, port, f"SET {key} {value}")
    print(f"    SET: {resp_set}")
    # Formato atteso: "OK version=<V> acks=<N>".
    if not resp_set or not resp_set.startswith("OK version="):
        print("    ✗ SET fallito")
        return False

    resp_get = send_command(host, port, f"GET {key}")
    print(f"    GET: {resp_get}")
    # Verifica minima: la risposta contiene il valore scritto e una versione.
    if resp_get and value in resp_get and "version=" in resp_get:
        print("    ✓ SET + GET OK")
        return True
    print("    ✗ GET non ritorna il valore scritto")
    return False


# ----------------------------------------------------------------------------
# TEST 4 — VERSIONI MONOTONE
# ----------------------------------------------------------------------------
# Scrivo 3 volte la stessa chiave e verifico che le versioni crescano.
# E' importante perche' il coordinator usa la versione per decidere "chi
# ha il dato piu' nuovo": se le versioni non crescessero in modo monotono,
# tutta la logica di scelta della versione massima si romperebbe.

def test_multiple_sets(host: str, port: int) -> bool:
    """Test 4: SET incrementali, versioni monotone (hint della traccia)."""
    print("\n[TEST 4] Multiple SETs - Monotonic versions")
    key = f"version_{int(time.time())}"
    versions = []

    # Tre SET di fila sulla stessa chiave.
    for i in range(3):
        value = f"value_{i}"
        resp = send_command(host, port, f"SET {key} {value}")
        if resp and "version=" in resp:
            # Estraggo il numero dopo "version=".
            ver_str = resp.split("version=")[1].split()[0]
            version = int(ver_str)
            versions.append(version)
            print(f"    SET #{i+1}: version={version}")
        else:
            print(f"    ✗ SET #{i+1} fallito")
            return False

    # Controllo stretto: ogni versione deve essere strettamente > della precedente.
    if versions[0] < versions[1] < versions[2]:
        print(f"    ✓ Versioni monotone: {versions}")
        return True
    print(f"    ✗ Versioni non monotone: {versions}")
    return False


# ----------------------------------------------------------------------------
# TEST 5 — READ REPAIR (proprieta' L1)  ⭐ TEST IMPORTANTE
# ----------------------------------------------------------------------------
# Questo e' il primo dei due test "stellari" del progetto.
# Domanda: il read repair funziona davvero, o e' solo teoria?
#
# Strategia:
# 1) Bypasso il coordinator e scrivo a mano una chiave con versione alta
#    su R1 e R2 (la regola locale della replica accetta una write se
#    version >= version_locale, quindi questa scrittura passa).
# 2) Lascio R3 senza la chiave (stato controllato di "replica indietro").
# 3) Faccio 5 GET sul coordinator. Con R=2 di N=3, due quorum su tre
#    includono R3: in quei casi il coordinator vede R3 stale e attiva
#    il read repair.
# 4) Alla fine leggo direttamente R3 e verifico che ora abbia la chiave.
# Se l'ha -> L1 dimostrata sperimentalmente.

def test_read_repair(host: str, port: int, replica_ports: list) -> bool:
    """Test 5: READ REPAIR effettivo (proprieta' L1 della traccia)."""
    print("\n[TEST 5] Read repair effettivo (L1)")

    key = f"repair_{int(time.time())}"
    value = "convergent_value"
    # Versione volutamente alta per non collidere con valori che il
    # coordinator potrebbe assegnare in test paralleli.
    high_version = 1000

    # --- Setup: portiamo R1 e R2 a v=1000, R3 vuota ---
    print(f"    Setup: write diretto (v={high_version}) su R1 e R2, R3 vuota")
    r1 = send_to_replica("127.0.0.1", replica_ports[0], {
        "type": "write", "key": key, "value": value, "version": high_version,
    })
    r2 = send_to_replica("127.0.0.1", replica_ports[1], {
        "type": "write", "key": key, "value": value, "version": high_version,
    })
    # Verifico che le scritture dirette siano andate a buon fine.
    if not r1 or r1.get("status") != "ACK" or not r2 or r2.get("status") != "ACK":
        print(f"    ✗ Setup fallito: r1={r1}, r2={r2}")
        return False

    # --- Pre-condizione: R3 deve davvero essere vuota per quella chiave ---
    pre = send_to_replica("127.0.0.1", replica_ports[2], {"type": "read", "key": key})
    if pre is None or pre.get("found"):
        # Se R3 ha gia' la chiave, il test non sta dimostrando nulla.
        print(f"    ✗ Pre-condizione errata su R3: {pre}")
        return False
    print(f"    R3 pre-GET: found=false (corretta pre-condizione)")

    # --- Esecuzione: 5 GET via coordinator per dare tempo al repair ---
    # 5 perche' i quorum sono casuali: in media circa 2/3 delle GET
    # includono R3 e in quei casi parte il repair.
    print("    Eseguo 5 GET sul coordinator per attivare il read repair")
    for i in range(5):
        resp = send_command(host, port, f"GET {key}")
        # Ogni GET deve comunque ritornare il valore corretto al client,
        # anche prima che R3 sia stata riparata: e' il punto di S1.
        if not resp or "OK" not in resp or value not in resp:
            print(f"    ✗ GET #{i+1} inatteso: {resp}")
            return False

    # --- Verifica finale: R3 ora deve avere la chiave alla versione alta ---
    post = send_to_replica("127.0.0.1", replica_ports[2], {"type": "read", "key": key})
    if (
        post is not None
        and post.get("found")
        and int(post.get("version", -1)) == high_version
        and str(post.get("value", "")) == value
    ):
        # ✓ Convergenza dimostrata: L1 verificata empiricamente.
        print(f"    ✓ R3 riparata: version={post['version']}, value={post['value']}")
        return True
    print(f"    ✗ R3 NON riparata dopo 5 GET: {post}")
    return False


# ----------------------------------------------------------------------------
# TEST 6 — NOT_FOUND
# ----------------------------------------------------------------------------
# Una GET su una chiave mai scritta deve dire chiaramente "non c'e'",
# non deve inventare un valore o restituire stringa vuota.

def test_not_found(host: str, port: int) -> bool:
    """Test 6: GET su chiave inesistente -> NOT_FOUND."""
    print("\n[TEST 6] GET non-existent key")
    # Chiave con timestamp per essere certi che non esista.
    key = f"nonexistent_{int(time.time())}"
    resp = send_command(host, port, f"GET {key}")
    if resp == "NOT_FOUND":
        print(f"    ✓ {resp}")
        return True
    print(f"    ✗ Atteso 'NOT_FOUND', ricevuto '{resp}'")
    return False


# ----------------------------------------------------------------------------
# TEST 7 — ERROR HANDLING SU COMANDI INCOMPLETI
# ----------------------------------------------------------------------------
# Verifica le precondizioni del contratto: se mando SET o GET senza
# argomenti, il coordinator deve rifiutare in modo esplicito, non
# comportarsi in modo ambiguo.

def test_error_empty_key(host: str, port: int) -> bool:
    """Test 7: precondizione del contratto - chiave vuota -> ERR usage."""
    print("\n[TEST 7] Error handling - Empty key")

    # Caso 1: SET senza argomenti.
    resp_set = send_command(host, port, "SET")
    if not resp_set or "ERR usage" not in resp_set:
        print(f"    ✗ SET dovrebbe restituire ERR usage, ricevuto: {resp_set}")
        return False
    print(f"    SET error: {resp_set}")

    # Caso 2: GET senza argomenti.
    resp_get = send_command(host, port, "GET")
    if not resp_get or "ERR usage" not in resp_get:
        print(f"    ✗ GET dovrebbe restituire ERR usage, ricevuto: {resp_get}")
        return False
    print(f"    GET error: {resp_get}")
    print("    ✓ Errore di sintassi corretto")
    return True


# ----------------------------------------------------------------------------
# TEST 8 — COMANDO SCONOSCIUTO
# ----------------------------------------------------------------------------
# Verifica che il sistema accetti solo i comandi del protocollo dichiarato
# (PING, STATUS, SET, GET, QUIT) e respinga tutto il resto in modo esplicito.

def test_unknown_command(host: str, port: int) -> bool:
    """Test 8: comando fuori contratto -> ERR unknown command."""
    print("\n[TEST 8] Unknown command")
    resp = send_command(host, port, "INVALID_CMD")
    if resp and "ERR unknown command" in resp:
        print(f"    ✓ {resp}")
        return True
    print(f"    ✗ Atteso 'ERR unknown command', ricevuto '{resp}'")
    return False


# ----------------------------------------------------------------------------
# TEST 9 — QUIT
# ----------------------------------------------------------------------------
# Verifica la chiusura ordinata della connessione: invio QUIT, mi aspetto
# "OK BYE", poi provo a mandare ancora qualcosa e verifico che la
# connessione sia davvero chiusa.

def test_quit(host: str, port: int) -> bool:
    """Test 9: QUIT - chiusura ordinata della connessione."""
    print("\n[TEST 9] QUIT - Graceful shutdown")
    try:
        # Qui usiamo una connessione persistente (non send_command) perche'
        # vogliamo riusarla DOPO il QUIT per verificare che sia chiusa.
        with socket.create_connection((host, port), timeout=2.0) as conn:
            conn_file = conn.makefile("rwb")
            conn_file.write(b"QUIT\n")
            conn_file.flush()
            resp = conn_file.readline().decode("utf-8", errors="replace").strip()

            if resp != "OK BYE":
                print(f"    ✗ Risposta QUIT inattesa: {resp}")
                return False

            # Secondo tentativo: la connessione deve essere chiusa.
            # Mi aspetto un'eccezione oppure una read vuota.
            try:
                conn_file.write(b"PING\n")
                conn_file.flush()
                resp2 = conn_file.readline()
                if resp2:
                    print(f"    ✗ Connessione non chiusa: ricevuto '{resp2}'")
                    return False
            except (socket.error, OSError, ConnectionResetError):
                # Comportamento atteso: la write su socket chiuso fallisce.
                pass

            print(f"    ✓ QUIT: {resp} (connessione chiusa correttamente)")
            return True
    except Exception as e:
        print(f"    ✗ Errore: {e}")
        return False


# ----------------------------------------------------------------------------
# TEST 10 — CONFLICT DETECTION (proprieta' S3)  ⭐ TEST IMPORTANTE
# ----------------------------------------------------------------------------
# Secondo test "stellare" del progetto.
# Domanda: cosa fa il sistema quando trova stessa versione + valori diversi?
# Risposta corretta: NON sceglie, ma avvisa il client.
#
# Strategia:
# 1) Bypasso il coordinator e scrivo direttamente sulle 3 repliche la
#    stessa chiave, stessa versione (9999) ma con 3 valori diversi.
# 2) Faccio GET via coordinator: qualunque quorum di 2 repliche su 3
#    vede sempre stessa versione con valori diversi.
# 3) Verifico che la risposta sia "ERR conflict version=9999", non un
#    valore preso a caso.

def test_conflict_detection(host: str, port: int, replica_ports: list) -> bool:
    """Test 10: rilevamento esplicito del conflitto (proprieta' S3 della traccia)."""
    print("\n[TEST 10] Conflict detection (S3)")
    key = f"conflict_{int(time.time())}"
    # Versione fissa e alta, uguale per tutte e 3 le repliche.
    conflict_version = 9999

    # --- Setup: stessa versione, valori diversi sulle 3 repliche ---
    print(f"    Setup: v={conflict_version} con valori diversi su tutte le repliche")
    for idx, replica_port in enumerate(replica_ports):
        resp = send_to_replica("127.0.0.1", replica_port, {
            "type": "write",
            "key": key,
            "value": f"value_replica_{idx}",  # ogni replica ha un valore diverso
            "version": conflict_version,
        })
        # La replica puo' rispondere ACK (prima scrittura) oppure ERR
        # (se gia' aveva quella versione con altro valore: la sua difesa
        # locale al conflitto e' anch'essa parte della proprieta' S3).
        # Per noi va bene in entrambi i casi: cio' che conta e' lo stato
        # finale, che e' "3 valori diversi alla stessa versione".
        if not resp or resp.get("status") not in ("ACK", "ERR"):
            print(f"    ✗ Setup R{idx+1} risposta inattesa: {resp}")
            return False

    # --- Esecuzione: GET via coordinator ---
    # Il coordinator vede 2 risposte con stessa versione ma valori diversi.
    # NON deve scegliere: deve restituire un errore esplicito.
    resp = send_command(host, port, f"GET {key}")
    if resp and "ERR conflict" in resp and f"version={conflict_version}" in resp:
        # ✓ Filosofia fail-safe rispettata: il sistema preferisce avvisare
        # il client piuttosto che restituire un dato non verificabile.
        print(f"    ✓ {resp}")
        return True
    print(f"    ✗ Atteso 'ERR conflict version={conflict_version}', ricevuto '{resp}'")
    return False


# ----------------------------------------------------------------------------
# RUNNER: esegue tutta la suite e stampa il riepilogo
# ----------------------------------------------------------------------------
# Ordine d'esecuzione:
#   1) Smoke (PING, STATUS)
#   2) Funzionalita' base (SET+GET, versioni)
#   3) Test "stellare" L1 (read repair)
#   4) Casi del contratto (NOT_FOUND, ERR usage, ERR unknown, QUIT)
#   5) Test "stellare" S3 (conflict detection) -- alla fine perche' lascia
#      le repliche in stato "sporco" che potrebbe interferire con test
#      successivi sulla stessa chiave.

def run_all_tests(host: str, port: int, replica_ports: list) -> None:
    """Esegue tutta la suite e stampa il riepilogo."""
    print("=" * 60)
    print("QUORUM STORE - TEST SUITE")
    print("=" * 60)
    print(f"Coordinator: {host}:{port}")
    print(f"Repliche:    {replica_ports}")

    # Raccolgo (nome, esito) per il riepilogo finale.
    results = []

    results.append(("PING", test_ping(host, port)))
    ok, _, _ = test_status(host, port)
    results.append(("STATUS", ok))

    results.append(("SET+GET nominal", test_set_get_nominal(host, port)))
    results.append(("Multiple SETs", test_multiple_sets(host, port)))
    results.append(("Read repair (L1)", test_read_repair(host, port, replica_ports)))
    results.append(("NOT_FOUND", test_not_found(host, port)))

    results.append(("Error handling", test_error_empty_key(host, port)))
    results.append(("Unknown command", test_unknown_command(host, port)))
    results.append(("QUIT", test_quit(host, port)))

    results.append(("Conflict detection (S3)", test_conflict_detection(host, port, replica_ports)))

    # Riepilogo: passa/totale e dettaglio per ogni test.
    print("\n" + "=" * 60)
    print("RIEPILOGO")
    print("=" * 60)
    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    for name, ok in results:
        status = "✓" if ok else "✗"
        print(f"{status} {name}")

    print(f"\nRisultato: {passed}/{total} test passati")
    # Exit code 0 se tutto passa, 1 altrimenti: utile per CI/script.
    if passed == total:
        print("✓ TUTTI I TEST PASSATI")
        sys.exit(0)
    else:
        print(f"✗ {total - passed} test falliti")
        sys.exit(1)


def main() -> None:
    # Tutti i parametri hanno default sensati: in demo basta "python test_scenarios.py".
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6420)
    parser.add_argument(
        "--replica-ports",
        nargs=3,
        type=int,
        default=[6421, 6422, 6423],
        help="Porte delle 3 repliche (usate dai test S3 e L1 per setup diretto)",
    )
    args = parser.parse_args()
    run_all_tests(args.host, args.port, args.replica_ports)


if __name__ == "__main__":
    main()
