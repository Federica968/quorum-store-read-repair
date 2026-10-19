# Quorum Store con Read Repair

Archivio chiave-valore distribuito su 3 repliche, con quorum di lettura e scrittura e **read repair automatico**: quando una `GET` trova repliche con versioni diverse, il coordinator risponde con la versione più recente e in background aggiorna le repliche rimaste indietro.

Progetto di gruppo del corso di **Architetture dei Sistemi Distribuiti** — Laurea Magistrale in Ingegneria dei Sistemi Intelligenti, Università Campus Bio-Medico di Roma (A.A. 2025/2026).

**Tecnologie:** Python 3 (solo libreria standard) · socket TCP · protocollo JSON line-based · `ThreadPoolExecutor` · test end-to-end

---

## Il team e il mio ruolo

| Ruolo | Persona | Responsabilità |
|---|---|---|
| **Protocol Owner** | **Federica Leacche** | Specifica dei messaggi e del protocollo, precondizioni e postcondizioni (Design by Contract), dimostrazione delle proprietà di safety del quorum, documentazione e parte dei test |
| Implementation Owner | Fabio Bosco | Architettura software, lock per chiave nel coordinator, parallelismo con `ThreadPoolExecutor` |
| Fault & Test Owner | Alessandro Napolitano | Test suite automatica, simulazione di guasti (nodi lenti, timeout, partizioni) |

Repository originale del gruppo: [Alessandro-Napolitano/progetto_architetture](https://github.com/Alessandro-Napolitano/progetto_architetture)

---

## Struttura della repo

```
quorum-store-read-repair/
├── README.md
├── docs/
│   └── presentazione.html    presentazione del progetto (apribile nel browser)
└── quorum_cluster/
    ├── coordinator.py        coordinator TCP: quorum, versioning, read repair
    ├── replica_node.py       replica TCP: storage in-memory + difesa locale
    ├── client.py             client interattivo testuale
    └── test_scenarios.py     suite di 10 test automatici end-to-end
```

Non servono librerie esterne: basta Python 3.9 o superiore.

---

## Architettura

| Componente   | Porta default | Ruolo |
|---|---|---|
| Coordinator  | 6420 | Orchestra il quorum, assegna le versioni, fa il read repair |
| Replica R1   | 6421 | Storage in-memory di coppie `(value, version)` |
| Replica R2   | 6422 | Storage in-memory di coppie `(value, version)` |
| Replica R3   | 6423 | Storage in-memory di coppie `(value, version)` |
| Client       | —    | Si collega al coordinator in TCP testuale |

Configurazione di default: `N = 3`, `R = 2`, `W = 2`. La condizione
`R + W > N` garantisce che ogni lettura intersechi sempre almeno una
replica che ha ricevuto l'ultima scrittura.

---

## Protocollo client → coordinator

| Comando | Risposta OK | Risposte ERR |
|---|---|---|
| `PING` | `OK PONG` | — |
| `STATUS` | `OK N=3 R=2 W=2` | — |
| `SET <key> <value>` | `OK version=<V> acks=<N>` | `ERR usage: SET <key> <value>` · `ERR write quorum not reached acks=<N>` · `ERR conflict version=<V>` |
| `GET <key>` | `OK <value> version=<V>` | `NOT_FOUND` · `ERR read quorum not reached responses=<N>` · `ERR conflict version=<V>` |
| `QUIT` | `OK BYE` | — |

Internamente, coordinator e repliche parlano in **JSON line-based su TCP**
(`{"type": "read", ...}` / `{"type": "write", ...}` / `{"status": "ACK", ...}`).

---

## Proprietà garantite

### Safety (il sistema non fa nulla di male)

- **S1 — Una GET non regredisce.** Il coordinator legge da almeno `R` repliche
  e ritorna sempre il valore con versione massima osservata.
- **S2 — Read repair monotono.** La replica accetta una `write` solo se la
  versione in arrivo è `>=` di quella locale: nessuna regressione possibile.
- **S3 — Conflitto esplicito.** Se due repliche hanno la stessa versione con
  valori diversi, il coordinator risponde `ERR conflict version=<V>` invece
  di scegliere un valore arbitrario.

### Liveness (il sistema progredisce se può)

- **L1 — Convergenza.** Le repliche stale convergono alla versione massima
  attraverso il read repair sulle letture successive (senza anti-entropy).
- **L2 — Tolleranza alla lentezza.** Le RPC sono parallele
  (`ThreadPoolExecutor` + `as_completed`): il GET ritorna appena arrivano
  `R` risposte, anche se una replica è lenta o irraggiungibile.

---

## Esecuzione

In 5 terminali separati, partendo dalla root del progetto:

```bash
# Terminale 1-3: le tre repliche
python3 quorum_cluster/replica_node.py --node-id R1 --port 6421
python3 quorum_cluster/replica_node.py --node-id R2 --port 6422
python3 quorum_cluster/replica_node.py --node-id R3 --port 6423

# Terminale 4: il coordinator (default N=3, R=2, W=2)
python3 quorum_cluster/coordinator.py --read-quorum 2 --write-quorum 2

# Terminale 5: il client interattivo
python3 quorum_cluster/client.py
```

Nel client:

```text
kv> PING
OK PONG
kv> STATUS
OK N=3 R=2 W=2
kv> SET k1 ciao
OK version=1 acks=2
kv> GET k1
OK ciao version=1
kv> QUIT
OK BYE
```

---

## Test automatici

```bash
python3 quorum_cluster/test_scenarios.py
```

Esegue 10 test contro il sistema reale (coordinator + 3 repliche devono
essere già attivi):

| # | Test | Proprietà verificata |
|---|---|---|
| 1 | `PING` | smoke |
| 2 | `STATUS` con `N=3 R=2 W=2` | precondizione L2 |
| 3 | `SET` + `GET` nominale | S1 (implicita) |
| 4 | 3 `SET` consecutivi → versioni monotone | S1 |
| 5 | **Read repair effettivo** (R3 vuota → converge dopo 5 GET) | **L1** + S2 |
| 6 | `GET` su chiave inesistente → `NOT_FOUND` | contratto |
| 7 | `SET` / `GET` senza argomenti → `ERR usage` | contratto |
| 8 | Comando ignoto → `ERR unknown command` | contratto |
| 9 | `QUIT` → connessione chiusa | contratto |
| 10 | **Conflict detection** (stessa versione, valori diversi) → `ERR conflict version=<V>` | **S3** |

I test 5 e 10 usano il protocollo JSON per scrivere direttamente sulle
repliche e portarle in uno stato controllato che il coordinator da solo non
saprebbe creare. Esito atteso: **10/10 test passati**.

---

## Limiti accettati

| Limite | Motivo | Evoluzione naturale |
|---|---|---|
| Storage in-memory | Semplicità didattica | WAL o embedded store (SQLite, RocksDB) |
| Single coordinator (SPOF) | Lab, non produzione | Raft multi-coordinator |
| Versioni intere monotone | Coordinator unico le rende sempre ordinabili | Vector clock se si passa a multi-master |
| Timeout RPC fisso a 1s | Prevedibile e ragionabile | Adattivo / circuit breaker |
| No Byzantine fault tolerance | Repliche oneste per assunzione | Firme HMAC o consensus BFT |
