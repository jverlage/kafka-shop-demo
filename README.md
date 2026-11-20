# Kafka Shop Demo

Ein Onlineshop als Live-Demo für Apache Kafka: Bestellungen gehen in ein Topic, fünf
unabhängige Services konsumieren sie, ein Dashboard macht alles sichtbar – in vier
zuschaltbaren Stufen von „Fan-out“ bis „Event-Kette“.

Das Dashboard zeigt den Prozess von links nach rechts: **Shop → Kafka → Consumer**. Jede
Nachricht wandert als Etikett über animierte Pfeile (Tempo einstellbar, Standard 1,6 s pro
Pfeil), in der Reihenfolge, in der die Daten tatsächlich fließen.

## Start

Voraussetzung: Docker Desktop (oder eine andere Docker-Engine mit Compose v2).

```bash
docker compose up -d --build
```

| Was | Wo |
|-----|----|
| Dashboard | http://localhost:3000 |
| Kafka UI (Blick unter die Haube) | http://localhost:8080 |
| Kafka-Broker für eigene Clients | `localhost:9094` |

Der erste Start dauert wegen der Image-Downloads ein paar Minuten, danach ~20 Sekunden.

Funktionstest (startet den Stack, schickt eine Bestellung, prüft alle Services):

```bash
./scripts/smoke-test.sh
```

Stoppen bzw. komplett aufräumen:

```bash
docker compose down
```

## Architektur

```
Shop/Simulator ─▶ gateway ─▶ orders ─┬─▶ inventory      (Lager)
                                     ├─▶ analytics      (Umsatz)
                                     └─▶ payment ─▶ payments ─▶ shipping ─▶ shipments ─▶ notification
Services ─▶ service-events ─▶ gateway ─▶ WebSocket ─▶ Dashboard
```

| Container | Rolle |
|-----------|-------|
| `kafka` | Apache Kafka 4.3 im KRaft-Modus (ohne ZooKeeper), ein Broker |
| `kafka-ui` | kafbat Kafka UI |
| `gateway` | Shop-API, Simulator, liest alle Topics mit, Lag über die Admin-API, liefert das Dashboard aus |
| `inventory`, `payment`, `analytics`, `shipping`, `notification` | je eine eigene Consumer Group; gleiche Codebasis, Rolle über `SERVICE_NAME` |

- Topics `orders`, `payments`, `shipments` haben je **3 Partitionen**, Key = Kunden-ID.
- Jeder Service kann intern 0–4 Consumer-Instanzen derselben Gruppe betreiben
  (4 > 3 Partitionen → eine Instanz bleibt sichtbar „idle“).
- Jede Verarbeitung hat eine künstliche Verzögerung (`app/src/shared/config.ts`, `delayMs`),
  damit Lag bei höherer Last sichtbar wird. `payment` ist mit 250 ms bewusst der Flaschenhals
  (~4 Bestellungen/s pro Instanz; Simulator-Standard 2/s, für die Lag-Demo 6/s).

## Projektstruktur

```
docker-compose.yml
app/
  src/shared/    Konfiguration (Topics, Kunden, Produkte, Services) und Kafka-Helfer
  src/service/   Service-Runtime (Instanzen, Heartbeat, HTTP-Steuerung) und Fachlogik
  src/gateway/   HTTP/WebSocket, Observer, Lag, Simulator, Topic-Reset
  public/        Dashboard (Vanilla JS, ohne Build-Schritt)
scripts/smoke-test.sh
java-client/     Minimaler Java-Producer/-Consumer gegen localhost:9094 (siehe java-client/README.md)
DEMO.md          Spickzettel für die Live-Demo
```

Node 24 führt die TypeScript-Dateien direkt aus (Type Stripping), es gibt keinen Build-Schritt.
`app/public` ist ins Gateway gemountet: Änderungen am Dashboard sind nach einem Browser-Reload
sichtbar. Änderungen am TypeScript-Code brauchen `docker compose up -d --build`.

Lokaler Typecheck: `cd app && npm install && npm run typecheck`.

## API (für eigene Experimente)

| Methode | Pfad | Body |
|---------|------|------|
| `POST` | `/api/orders` | `{ "customerId"?: "anna", "productId"?: "hoodie" }` |
| `POST` | `/api/orders/burst` | `{ "count": 20 }` |
| `POST` | `/api/simulator` | `{ "enabled"?: true, "rate"?: 5 }` |
| `POST` | `/api/services/<service>/<start\|stop\|scale-up\|scale-down>` | – |
| `POST` | `/api/reset` | – |
| `GET`  | `/api/state` | – |

## Bekannte Eigenheit

`@confluentinc/kafka-javascript` (librdkafka) stürzt mit einem Segfault ab, wenn
`admin.fetchOffsets` aufgerufen wird, während der Group Coordinator eines frisch
gestarteten Clusters noch lädt. Das Gateway startet das Lag-Polling deshalb erst, wenn
sein eigener Consumer Partitionen zugewiesen bekommen hat.

## Lizenz

[MIT](LICENSE) – frei nutzbar, auch für Lehre und eigene Weiterentwicklungen.
