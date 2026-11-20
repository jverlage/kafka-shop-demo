# Live-Demo – Spickzettel (5 Minuten)

## Vorher (vor dem Vortrag)

- [ ] Docker Desktop läuft, `docker compose up -d --build` – ~1 Min vor Beginn fertig
- [ ] Dashboard http://localhost:3000 im Vollbild (`F11` bzw. `⌃⌘F`)
- [ ] Ein Terminal-Fenster im Projektordner offen (für den Ausfall per `docker compose stop`)
- [ ] **↺ Reset** klicken → Bestellnummern starten bei B-1001, alles bei 0
- [ ] Stufe **1** gewählt, Simulator **aus**, Tempo **1,6 s**
- [ ] Optional: Kafka UI http://localhost:8080 in einem zweiten Tab

Tasten: `1`–`4` Stufe · `B` Bestellung · `S` Simulator an/aus · `X` +20 Bestellungen

Das Bild ist immer gleich: **Shop → Kafka → Consumer**. Grüne Pfeile = *produce* (schreibt
nach Kafka), lila Pfeile = *consume* (liest aus Kafka). Jede Bestellung wandert als Etikett
über die Pfeile; der Zähler eines Service springt erst, wenn das Etikett angekommen ist.
Das Tempo regelst du unten in der Leiste.

---

## 0:00 – Stufe 1 · Fan-out (≈ 1 Min)

1. Ein Produkt anklicken, z. B. **Kafka-Hoodie**.
2. Mitlesen lassen: Etikett läuft in Kafka, landet im Topic `orders` – dann laufen **drei**
   Etiketten gleichzeitig zu Lager, Analytics und Zahlung.
3. Noch 1–2 Bestellungen klicken.

> „Der Shop schreibt die Bestellung genau **einmal** in Kafka – und weiß nicht, wer sie liest.
> Lager, Analytics und Zahlung lesen sie unabhängig voneinander. Ein neuer Service?
> Einfach mitlesen lassen, am Shop ändert sich nichts.“

## 1:00 – Stufe 2 · Partitionen (≈ 1 Min)

1. Taste `2`. Das Topic hat jetzt 3 Spuren (P0–P2), die Etiketten haben die Farbe des Kunden.
2. Im Shop **Anna** wählen, zwei-, dreimal bestellen → landet immer in derselben Spur
   (am Kunden steht „→ P1“).
3. **Zufall** wählen, `X` drücken → alle drei Spuren füllen sich.

> „Der Key – hier der Kunde – bestimmt die Partition. Alle Bestellungen von Anna liegen in
> derselben Partition, also bleibt ihre Reihenfolge garantiert. Partitionen sind außerdem
> das, was Kafka skalierbar macht – dazu gleich mehr.“

## 2:00 – Stufe 3 · Consumer Groups (≈ 1,5 Min)

1. Taste `3`. Jede Karte zeigt jetzt Instanzen (welche Partitionen sie lesen), Lag und Knöpfe.
2. **Ausfall:** im Terminal
   ```bash
   docker compose stop inventory
   ```
   (oder ■ Stop auf der Lager-Karte) → dann `X` drücken. Der Pfeil zum Lager wird grau,
   „Lag“ zählt hoch – die anderen Services laufen ungestört weiter.
3. **Wiederkehr:**
   ```bash
   docker compose start inventory
   ```
   (oder ▶ Start) → die Etiketten laufen nacheinander zum Lager, Lag fällt auf 0.
4. Optional **Skalieren:** bei Zahlung zweimal **+** → aus `#1 P0 P1 P2` wird
   `#1 P0 · #2 P1 · #3 P2` (Rebalance). Noch ein **+** → die vierte Instanz ist **idle**,
   denn es gibt nur 3 Partitionen.

> „Kafka merkt sich pro Consumer Group, wie weit sie gelesen hat. Fällt ein Service aus,
> warten die Nachrichten einfach – nichts geht verloren. Und Partitionen sind die Einheit der
> Parallelität: maximal so viele aktive Instanzen wie Partitionen.“

## 3:30 – Stufe 4 · Event-Kette (≈ 1 Min)

1. Taste `4`. Im Broker erscheinen die Topics `payments` und `shipments`.
2. Eine Bestellung klicken und dem Weg folgen: Zahlung schreibt **zurück** nach `payments` →
   Versand liest und schreibt nach `shipments` → Benachrichtigung verschickt die E-Mail.
3. Optional Simulator an (`S`) mit **1–2/s** – dann läuft die ganze Kette als Strom.

> „Kein Service ruft einen anderen direkt auf. Jeder reagiert auf Events und erzeugt neue.
> Fällt der Versand aus, wird trotzdem weiter bezahlt – die Pakete gehen raus, sobald er
> wieder da ist.“

## 4:30 – Abschluss (≈ 30 s)

- Simulator aus (`S`).
- Optional kurz **Kafka UI** (:8080) zeigen: Topics, Partitionen, Consumer Groups – „das ist
  dasselbe, was das Dashboard visualisiert“.

---

## Wenn die Zeit knapp ist

| Restzeit | Zeigen |
|----------|--------|
| 2 Min | Stufe 1 → direkt Stufe 3 (Ausfall + Wiederkehr) |
| 3 Min | Stufe 1 → 2 → 3 (ohne Skalieren) |
| 5 Min | alles |

Die Stufen sind unabhängig: Im Hintergrund läuft immer alles, die Stufe bestimmt nur, was zu
sehen ist. Überspringen ist jederzeit möglich.

## Notfall

| Problem | Lösung |
|---------|--------|
| Dashboard zeigt „getrennt“ | `docker compose ps` – Gateway läuft? Sonst `docker compose up -d` |
| Service-Karte „offline“, Start-Button tut nichts | Container ist gestoppt: `docker compose start <service>` |
| Durcheinander nach Experimenten | **↺ Reset** (setzt Topics und Zähler zurück, ~1 s) |
| Gar nichts geht mehr | `docker compose down && docker compose up -d` (~30 s) |

## Bonus für Fragen aus dem Publikum

Lag direkt aus Kafka (ohne Dashboard):
```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group payment
```

Rohe Nachrichten mit Key und Partition mitlesen:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --formatter-property print.key=true --formatter-property print.partition=true
```
