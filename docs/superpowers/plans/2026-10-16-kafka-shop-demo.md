# Kafka Shop Demo – Implementation Plan

**Goal:** Lokales Kafka-Demo-Setup (docker compose) mit Onlineshop-Simulation, fünf Consumer-Services und Live-Dashboard in vier zuschaltbaren Stufen.

**Architecture:** Ein Node-Image (`app/`) für Gateway und Services, Rolle per Kommando bzw. `SERVICE_NAME`. Gateway produziert Bestellungen, liest alle Topics + `service-events` mit, berechnet Lag via Admin-API und pusht per WebSocket an ein statisches Vanilla-JS-Dashboard. Services steuerbar per HTTP.

**Tech Stack:** apache/kafka 4.3.1 (KRaft), kafbat/kafka-ui v1.5.0, Node 24 (native TS type stripping, kein Build), @confluentinc/kafka-javascript 1.11.0, ws.

Spec: `docs/superpowers/specs/2026-10-16-kafka-shop-demo-design.md`

## Global Constraints

- Start mit genau einem Befehl: `docker compose up -d --build`
- Dashboard `http://localhost:3000`, kafka-ui `http://localhost:8080`, Kafka extern `localhost:9094`
- Topics `orders`, `payments`, `shipments` mit je 3 Partitionen, Key = `customerId`; `service-events`
- `auto.create.topics.enable=false` – Topics legt nur das Gateway an
- Max. 4 Instanzen pro Service (eine mehr als Partitionen → idle Consumer sichtbar)
- UI-Texte auf Deutsch, Beamer-tauglich

## File Structure

```
docker-compose.yml
app/
  Dockerfile, package.json, tsconfig.json, .dockerignore
  src/shared/config.ts      – Brokers, Topics, Produkte, Kunden, Service-Definitionen
  src/shared/kafka.ts       – Kafka-Client-Factory, waitForTopics
  src/service/main.ts       – Service-Runtime: Instanzen, Heartbeat, HTTP /control
  src/service/handlers.ts   – Fachlogik je Service (inventory, payment, ...)
  src/gateway/main.ts       – HTTP + WebSocket + Verdrahtung
  src/gateway/topics.ts     – ensureTopics, resetTopics
  src/gateway/observer.ts   – liest Topics + service-events → Broadcast
  src/gateway/lag.ts        – Lag-Polling
  src/gateway/simulator.ts  – Auto-Bestellungen
  public/index.html, styles.css, app.js
scripts/smoke-test.sh
README.md, DEMO.md
```

## Tasks

### Task 1: Infrastruktur + Shared
- compose mit kafka (Healthcheck), kafka-ui, gateway, 5 Services
- `config.ts`, `kafka.ts`, Dockerfile
- Verify: `docker compose up -d kafka` → healthy

### Task 2: Services
- Runtime mit Instanz-Management (start/stop/scale), Assignment-Polling, Heartbeat
- Handler: inventory (Lagerbestand), payment (→ payments), shipping (→ shipments), notification, analytics (Umsatz)
- Verify: Logs zeigen Verarbeitung nach manueller Bestellung

### Task 3: Gateway
- REST: `POST /api/orders`, `/api/orders/burst`, `/api/simulator`, `/api/services/:name/:action`, `/api/reset`
- WS: `snapshot`, `message`, `processed`, `status`, `lag`, `simulator`, `reset`
- Verify: `curl -XPOST localhost:3000/api/orders` + WS-Client sieht Events

### Task 4: Frontend
- Stufen 1–4, Shop, Topic-Ansicht (Liste/Spuren), Service-Karten, Lag, Instanzen, Timeline
- Verify: Browser-Check aller Stufen

### Task 5: Smoke-Test + Doku
- `scripts/smoke-test.sh`: Bestellung → alle 5 Services verarbeitet
- README (Setup), DEMO.md (5-Minuten-Ablauf)
