#!/usr/bin/env bash
# Startet den Stack, setzt die Demo zurück, schickt eine Bestellung und prüft,
# dass alle fünf Services sie verarbeitet haben.
set -euo pipefail
cd "$(dirname "$0")/.."

GATEWAY=http://localhost:3000
SERVICES="inventory payment analytics shipping notification"

echo "▶ Stack starten …"
docker compose up -d --build --wait >/dev/null 2>&1

echo "▶ Warte auf Gateway …"
for _ in $(seq 1 60); do
  curl -sf "$GATEWAY/api/state" >/dev/null && break
  sleep 1
done

echo "▶ Warte, bis alle Services online sind …"
for _ in $(seq 1 60); do
  online=$(curl -sf "$GATEWAY/api/state" | python3 -c 'import sys,json; print(sum(s["online"] for s in json.load(sys.stdin)["services"]))')
  [ "$online" = "5" ] && break
  sleep 1
done
[ "$online" = "5" ] || { echo "✗ Nur $online/5 Services online"; exit 1; }

echo "▶ Reset …"
curl -sf -XPOST "$GATEWAY/api/reset" >/dev/null

order=$(curl -sf -XPOST "$GATEWAY/api/orders" -H 'content-type: application/json' -d '{"customerId":"anna","productId":"hoodie"}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["orderId"])')
echo "▶ Bestellung $order gesendet, warte auf Verarbeitung …"

for _ in $(seq 1 30); do
  done_by=$(curl -sf "$GATEWAY/api/state" | python3 -c "
import sys, json
s = json.load(sys.stdin)
print(' '.join(sorted({e['service'] for e in s['recent'] if e['type'] == 'processed' and e['orderId'] == '$order'})))")
  missing=""
  for service in $SERVICES; do
    [[ " $done_by " == *" $service "* ]] || missing="$missing $service"
  done
  [ -z "$missing" ] && { echo "✓ $order wurde von allen Services verarbeitet: $done_by"; exit 0; }
  sleep 1
done

echo "✗ Nicht verarbeitet von:$missing"
exit 1
