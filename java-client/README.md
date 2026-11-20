# Java-Client – Producer & Consumer in 30 Zeilen

Minimalbeispiel mit dem offiziellen `kafka-clients`: eine Klasse schreibt eine Bestellung nach
Kafka, eine liest sie. Beide hängen sich an den Broker des Shop-Demonstrators
(`localhost:9094`) und das bestehende Topic `orders` – eine Java-Bestellung läuft also auch
sichtbar durchs Dashboard und wird von Lager, Zahlung & Co. verarbeitet.

| Datei | Was |
|-------|-----|
| [`OrderProducer.java`](src/main/java/demo/OrderProducer.java) | Konfiguration → `KafkaProducer` → `send(topic, key, value)` |
| [`OrderConsumer.java`](src/main/java/demo/OrderConsumer.java) | Konfiguration inkl. `group.id` → `subscribe` → `poll`-Schleife |

Voraussetzung: der Stack läuft (`docker compose up -d` im Projekt-Root) und ein JDK ≥ 21.
Maven muss nicht installiert sein, `./mvnw` lädt es beim ersten Aufruf.

## Starten

Terminal 1 – Consumer (läuft, bis `Strg+C`):

```bash
./mvnw -q compile exec:exec@consumer
```

Terminal 2 – Producer (schickt eine Bestellung und beendet sich):

```bash
./mvnw -q compile exec:exec@producer -Dcustomer=ben
```

Alternativ in IntelliJ: `java-client` als Maven-Projekt öffnen und die `main`-Methoden über
den grünen Pfeil starten (Programmargument = Kunde, Standard `anna`).

## Ablauf für die Live-Demo (≈ 2 Min)

1. **Producer-Code zeigen** – drei Schritte: Properties (Broker + Serializer), Producer
   erzeugen, `send` mit Topic, **Key** und Value.
2. Producer starten → Ausgabe zeigt Partition und Offset, im Dashboard wandert `J-…` durch
   Kafka zu den Services.
3. Mit gleichem Kunden noch einmal senden → **gleiche Partition** (Key bestimmt die Partition).
4. **Consumer-Code zeigen** – neu ist nur `group.id` und die `poll`-Schleife.
5. Consumer starten → liest als neue Group `java-demo` erst einmal **alle bisherigen
   Bestellungen** nach (`auto.offset.reset=earliest`), dann live jede neue.
6. Consumer mit `Strg+C` stoppen, zwei Bestellungen produzieren, Consumer neu starten →
   er liefert genau die verpassten nach: Kafka hat sich den Offset der Group gemerkt.

> „Ein neuer Service ist eine neue Consumer Group – am Shop ändert sich nichts.“

## Gut zu wissen

- Nach dem Neustart dauert es bis zu ~6 s, bis der Consumer wieder liest: Ohne sauberes
  `close()` (Strg+C) wartet Kafka den `session.timeout.ms` ab, bevor die Group neu verteilt.
  Ohne die Zeile im Code wären es 45 s.
- Nochmal alles von vorne lesen: in `OrderConsumer` eine neue `group.id` eintragen.
- **↺ Reset** im Dashboard löscht die Topics; danach einfach Consumer neu starten.
- Die Bestellnummer `J-…` stammt aus der Uhrzeit, damit sie im Dashboard eindeutig ist.
