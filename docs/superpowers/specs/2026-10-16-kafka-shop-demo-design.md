# Kafka Shop Demo – Design

## Ziel

Ein lokal per `docker compose up` startbares Setup für eine 5-minütige Live-Demo im
Anschluss an einen 10-minütigen Vortrag über Apache Kafka. Ein fiktiver Onlineshop
erzeugt Bestellungen, mehrere Services konsumieren sie. Ein Web-Dashboard visualisiert
den Datenfluss. Die gezeigte Komplexität lässt sich in vier Stufen zuschalten.

## Architektur

| Container       | Aufgabe |
|-----------------|---------|
| `kafka`         | `apache/kafka` (4.x), KRaft, Single Broker, intern `kafka:9092`, extern `localhost:9094` |
| `kafka-ui`      | kafbat/kafka-ui auf `localhost:8080` – optionaler Blick unter die Haube |
| `gateway`       | Node/TS. Liefert Frontend aus (`localhost:3000`), REST für Bestellungen/Steuerung, WebSocket für Live-Events, Admin-API für Lag & Gruppen, Auto-Simulator |
| `inventory`     | konsumiert `orders` (Gruppe `inventory`) |
| `payment`       | konsumiert `orders` (Gruppe `payment`), produziert `payments` |
| `shipping`      | konsumiert `payments` (Gruppe `shipping`), produziert `shipments` |
| `notification`  | konsumiert `shipments` (Gruppe `notification`) |
| `analytics`     | konsumiert `orders` (Gruppe `analytics`) |

Alle Services teilen eine Codebasis (`services/`), die Rolle wird per `SERVICE_NAME` gesetzt.
Client-Bibliothek: `@confluentinc/kafka-javascript` (KafkaJS-kompatible API, gepflegt).

### Topics

- `orders` – 3 Partitionen, Key = `customerId`
- `payments` – 3 Partitionen, Key = `customerId`
- `shipments` – 3 Partitionen, Key = `customerId`
- `service-events` – Telemetrie der Services an das Gateway (verarbeitet, Instanz gestartet/gestoppt, Partitionszuordnung)

### Datenfluss

```
Shop/Simulator ─▶ gateway ─▶ orders ─┬─▶ inventory
                                     ├─▶ analytics
                                     └─▶ payment ─▶ payments ─▶ shipping ─▶ shipments ─▶ notification
Services ─▶ service-events ─▶ gateway ─▶ WebSocket ─▶ Dashboard
```

Das Gateway liest `orders`, `payments`, `shipments` mit einer eigenen Gruppe mit, damit das
Dashboard Partition und Offset jeder Nachricht zeigen kann. Zusätzlich liest es
`service-events`, um anzuzeigen, welcher Service welche Nachricht verarbeitet hat.

### Steuerung der Services

Jeder Service-Container betreibt intern 0..n Consumer-Instanzen (max. 4) derselben Gruppe
und bietet eine kleine HTTP-Steuer-API (`POST /control`). Das Gateway leitet Klicks im
Dashboard dorthin weiter. Kommandos: `stop` (alle Instanzen trennen sich → Gruppe leer,
Lag steigt), `start` (eine Instanz verbinden), `scale-up` (+1 Instanz → Rebalance),
`scale-down`, sowie intern `pause-all`/`resume` für den Reset. HTTP statt Kafka, weil
Steuerung synchron und zuverlässig sein muss. Damit braucht das Gateway keinen
Docker-Socket. Ein echtes `docker compose stop <service>` im Terminal funktioniert
zusätzlich und zeigt dasselbe Bild (Heartbeat bleibt aus → Service „offline“).

Die Services senden jede Sekunde einen Heartbeat (Instanzen, zugewiesene Partitionen,
Kennzahlen) und pro verarbeiteter Nachricht ein `processed`-Event an `service-events`.

Jede Verarbeitung hat eine künstliche Verzögerung (~300 ms, pro Service konfigurierbar),
damit Lag bei hoher Simulator-Rate sichtbar wird.

### Lag

Das Gateway fragt jede Sekunde per Admin-API Committed Offsets je Gruppe und die
High-Watermarks je Partition ab und sendet `lag` pro Gruppe und Partition per WebSocket.

## Frontend

Ein einzelnes, statisches Single-Page-Dashboard (Vanilla JS, kein Framework, vom
Gateway ausgeliefert). Neutrales Design, Light/Dark, für Beamer gut lesbar. Leitidee:
**ein Prozessbild, pro Stufe nur ein neuer Gedanke**.

Layout (alle Stufen): links **Shop** → Mitte groß **Kafka-Broker** (Fokus) → rechts **Consumer**.
Verbunden durch Pfeile (SVG), die gestrichelt „laufen“, solange etwas darüber wandert.
Produce-Pfeile und Consume-Pfeile haben je eine Farbe.

Animation: Jede Nachricht wandert als Etikett (Bestellnummer) über ihren Pfeil, Standard
1,6 s pro Pfeil, im Dashboard einstellbar. Ein Zähler springt erst, wenn das Etikett
angekommen ist. Die Abfolge folgt dem echten Datenfluss: Shop → Topic, erst danach
Topic → Consumer; ein Folge-Event (z. B. `payments`) startet erst, wenn das Etikett beim
produzierenden Service angekommen ist. Pro Pfeil laufen Etiketten mit Mindestabstand
hintereinander; staut es sich zu sehr (Simulator), wird die Animation für einzelne
Nachrichten übersprungen und nur der Effekt angewendet.

Service-Karten zeigen nur Icon, Name, Zähler und die letzte Aktion. Steuerung für den
Vortragenden (Simulator, +20, Tempo, Reset) liegt in einer schmalen Leiste unten.

### Stufen (Umschalter 1–4 oben, auch per Tastatur 1–4)

1. **Fan-out** – Shop (Produkte), Topic `orders` als ein Log, Consumer Lager/Zahlung/Analytics.
2. **Partitionen & Keys** – Topic als 3 Spuren, Etiketten in Kundenfarbe, Kundenauswahl im Shop mit „→ Partition“.
3. **Consumer Groups & Lag** – an den Karten Status, Instanzen mit Partitionen, Lag, Stop/Start, ±Instanz; Pfeil eines gestoppten Services wird grau.
4. **Event-Kette** – Topics `payments`/`shipments` im Broker; Zahlung und Versand schreiben per Pfeil zurück in Kafka; Versand und Benachrichtigung als weitere Consumer.

Das Backend läuft immer vollständig, die Stufe steuert nur die Sichtbarkeit.

### Reset

Löscht und erstellt die Topics neu und setzt Zähler zurück. Die Services verbinden ihre
Consumer nach dem Reset neu.

## Fehlerbehandlung

- Services und Gateway warten beim Start mit Retry auf Kafka (Healthcheck in Compose).
- Das Frontend verbindet den WebSocket bei Abbruch automatisch neu und zeigt den Verbindungsstatus.
- Fehler bei einzelnen Nachrichten werden geloggt, der Consumer läuft weiter.

## Test

- `scripts/smoke-test.sh`: Stack starten, eine Bestellung per REST senden, prüfen, dass
  inventory, payment, analytics, shipping und notification sie verarbeitet haben.
- Manueller Browser-Check: alle vier Stufen, Stop → Lag steigt → Start → Lag fällt, Scale-up → Rebalance.

## Nicht im Umfang

Persistenz außerhalb von Kafka, Authentifizierung, Multi-Broker-Cluster, Schema Registry.
