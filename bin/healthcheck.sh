#!/usr/bin/env bash
# loadwatch healthcheck — always exits 0, prints exactly ONE line.
#   OK <age>s              -> healthy, newest sample <age> seconds old
#   FAIL <reason>          -> something is wrong
# Intended to be run on the runtime host (dev CT141), e.g. over SSH from a watchdog.
set -u

cd "$(dirname "$0")/.." || { echo "FAIL cannot cd to project dir"; exit 0; }

ps_out=$(docker compose ps --format '{{.Service}}={{.State}}' 2>/dev/null | tr '\n' ' ')
if [ -z "$ps_out" ]; then
  echo "FAIL docker compose reports no services (stack down?)"
  exit 0
fi
for svc in db collector api; do
  case " $ps_out " in
    *" $svc=running "*) : ;;
    *) echo "FAIL service $svc is not running (states: $ps_out)"; exit 0 ;;
  esac
done

age=$(docker compose exec -T db psql -U loadwatch -d loadwatch -t -A -c \
  "select coalesce(extract(epoch from (now()-max(ts)))::int,-1) from samples;" 2>/dev/null | tr -d '[:space:]')
case "${age:-x}" in
  ''|*[!0-9-]*) echo "FAIL sample-age query failed (raw='${age:-empty}')" ;;
  -1)           echo "FAIL no samples in the database" ;;
  *) if [ "$age" -gt 300 ]; then
       echo "FAIL newest sample is ${age}s old (threshold 300s)"
     else
       echo "OK ${age}s"
     fi ;;
esac
exit 0
