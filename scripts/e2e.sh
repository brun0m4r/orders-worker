#!/usr/bin/env bash
# Test end to end contra el stack levantado con `docker compose up --build -d`.
# Uso: ./scripts/e2e.sh        (COMPOSE="docker-compose" si usás el binario standalone)
set -u

COMPOSE=${COMPOSE:-docker compose}
RUN=$(date +%s)
PASS=0; FAIL=0
KAFKA=/opt/kafka/bin

pass() { echo "  ✅ $1"; PASS=$((PASS+1)); }
fail() { echo "  ❌ $1"; FAIL=$((FAIL+1)); }
section() { echo; echo "▶ $1"; }

send() { # send <orderId> <json>
  echo "$1:$2" | docker exec -i orders-kafka $KAFKA/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic orders \
    --property parse.key=true --property key.separator=: 2>/dev/null
}
mongo_count() { docker exec orders-mongo mongosh orders --quiet --eval "db.orders.countDocuments({orderId:'$1'})" 2>/dev/null | tail -1; }
mongo_total() { docker exec orders-mongo mongosh orders --quiet --eval "var d=db.orders.findOne({orderId:'$1'}); print(d ? d.total.toString()+'|'+d.products[0].name+'|'+d.customerName : '')" 2>/dev/null | tail -1; }
redis() { docker exec orders-redis redis-cli "$@" 2>/dev/null; }
failed_field() { redis get "orders:failed:$1" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$2'))" 2>/dev/null; }

wait_for() { # wait_for <segundos> <comando...>  -> 0 si el comando tiene éxito antes del plazo
  local t=$1; shift
  for _ in $(seq 1 "$t"); do eval "$@" >/dev/null 2>&1 && return 0; sleep 1; done
  return 1
}

order_json() { # order_json <orderId> <customerId> <productId>:<qty>...
  local id=$1 cust=$2; shift 2
  local items=""
  for p in "$@"; do items+="{\"productId\":\"${p%%:*}\",\"quantity\":${p##*:}},"; done
  echo "{\"orderId\":\"$id\",\"customerId\":\"$cust\",\"products\":[${items%,}]}"
}

echo "Esperando que el worker esté listo..."
wait_for 90 'curl -sf localhost:8080/actuator/health | grep -q UP' || { echo "worker no responde"; exit 1; }
# asegura que el topic exista y el consumidor tenga particiones asignadas
send "warmup-$RUN" "$(order_json warmup-$RUN customer-456 product-789:1)"
wait_for 60 "[ \"\$(mongo_count warmup-$RUN)\" = 1 ]" || { echo "el worker no procesa mensajes"; exit 1; }

section "1. Pedido válido se enriquece y se guarda en Mongo"
O1=e2e-ok-$RUN
send $O1 "$(order_json $O1 customer-456 product-789:1 product-790:2)"
if wait_for 30 "[ \"\$(mongo_count $O1)\" = 1 ]"; then
  r=$(mongo_total $O1)
  [ "$r" = "1049|Laptop|Ana Pérez" ] && pass "guardado con total 1049, producto Laptop y cliente Ana Pérez" || fail "documento inesperado: '$r'"
else fail "el pedido no llegó a Mongo"; fi

section "2. Idempotencia: el mismo pedido dos veces se guarda una sola vez"
send $O1 "$(order_json $O1 customer-456 product-789:1 product-790:2)"
sleep 6
[ "$(mongo_count $O1)" = 1 ] && pass "sigue habiendo 1 documento" || fail "documentos: $(mongo_count $O1)"

section "3. Cliente inactivo: REJECTED en Redis, nada en Mongo"
O3=e2e-inactive-$RUN
send $O3 "$(order_json $O3 customer-999 product-789:1)"
wait_for 30 "[ \"\$(failed_field $O3 status)\" = REJECTED ]" && pass "estado REJECTED en Redis ($(failed_field $O3 lastError))" || fail "no quedó REJECTED"
[ "$(mongo_count $O3)" = 0 ] && pass "no se guardó en Mongo" || fail "se guardó en Mongo"
[ "$(redis zscore orders:failed:retry-queue $O3)" = "" ] && pass "no está en la cola de reintentos" || fail "está en la cola de reintentos"

section "4. Producto inexistente: REJECTED"
O4=e2e-noproduct-$RUN
send $O4 "$(order_json $O4 customer-456 product-789:1 product-000:1)"
wait_for 30 "[ \"\$(failed_field $O4 status)\" = REJECTED ]" && pass "estado REJECTED ($(failed_field $O4 lastError))" || fail "no quedó REJECTED"

section "5. Mensaje ilegible: se envía al DLQ (se verifica al final)"
echo "bad-$RUN:{esto no es json" | docker exec -i orders-kafka $KAFKA/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders --property parse.key=true --property key.separator=: 2>/dev/null
echo "  mensaje enviado"

section "6. API de enriquecimiento caída: reintentos en Redis y recuperación"
O6=e2e-retry-$RUN
$COMPOSE stop enrichment-api >/dev/null 2>&1
send $O6 "$(order_json $O6 customer-457 product-791:1)"
wait_for 40 "[ \"\$(failed_field $O6 status)\" = RETRYING ]" && pass "queda RETRYING (intentos: $(failed_field $O6 attempts))" || fail "no quedó RETRYING"
# con la API caída cada intento tarda unos segundos (timeouts + backoff), por eso la ventana es amplia
wait_for 60 "[ \"\$(failed_field $O6 attempts)\" -ge 2 ] 2>/dev/null" \
  && pass "el contador sigue subiendo con backoff (intentos: $(failed_field $O6 attempts))" \
  || fail "el contador no avanzó (intentos: $(failed_field $O6 attempts))"
$COMPOSE start enrichment-api >/dev/null 2>&1
if wait_for 90 "[ \"\$(mongo_count $O6)\" = 1 ]"; then pass "al volver la API, el pedido se procesó y está en Mongo"; else fail "no se recuperó"; fi
wait_for 10 "[ \"\$(redis exists orders:failed:$O6)\" = 0 ]" && pass "se limpió el registro de fallo en Redis" || fail "quedó el registro de fallo"

section "7. Lock distribuido: pedido bloqueado por otra instancia se omite"
O7=e2e-lock-$RUN
redis set "lock:order:$O7" other-instance PX 20000 >/dev/null
send $O7 "$(order_json $O7 customer-456 product-792:1)"
sleep 8
[ "$(mongo_count $O7)" = 0 ] && pass "no se procesó mientras otro tenía el lock" || fail "se procesó pese al lock"
$COMPOSE logs worker 2>&1 | grep -q "Order $O7 is being processed by another instance" && pass "el worker lo registró como omitido" || fail "sin log de omisión"
redis del "lock:order:$O7" >/dev/null
[ "$(redis exists lock:order:$O7)" = 0 ] && pass "(limpieza) lock eliminado"

section "8. Redis caído: el pedido no se pierde ni cuelga al consumidor (DLQ, se verifica al final)"
O8=e2e-redisdown-$RUN
docker stop orders-redis >/dev/null
sleep 2
send $O8 "$(order_json $O8 customer-456 product-789:1)"
sleep 10
[ "$(mongo_count $O8)" = 0 ] && pass "no se procesó sin poder tomar el lock" || fail "se procesó sin Redis"
docker start orders-redis >/dev/null

section "9. Tras volver Redis, el worker sigue procesando"
# Lettuce reconecta con espera creciente: los pedidos enviados durante esa ventana van al DLQ (no se pierden),
# así que se envían pedidos de prueba hasta que uno se procese.
recovered=0
for i in $(seq 1 12); do
  O9=e2e-after-$RUN-$i
  send $O9 "$(order_json $O9 customer-456 product-789:1)"
  if wait_for 5 "[ \"\$(mongo_count $O9)\" = 1 ]"; then recovered=1; break; fi
done
[ $recovered = 1 ] && pass "el worker se recuperó y procesó el pedido $O9" || fail "el worker no se recuperó tras volver Redis"

section "10. Contenido del DLQ (orders-dlq)"
dlq=$(docker exec orders-kafka $KAFKA/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders-dlq --from-beginning --timeout-ms 20000 --property print.headers=true --property print.key=true 2>/dev/null)
echo "$dlq" | grep "bad-$RUN" | grep -q "dlq-reason:unparseable" \
  && pass "mensaje ilegible presente, con dlq-reason:unparseable" || fail "falta el mensaje ilegible en el DLQ"
echo "$dlq" | grep "$O8" | grep -q "dlq-reason:failure-store-unavailable" \
  && pass "pedido con Redis caído presente, con dlq-reason:failure-store-unavailable" || fail "falta el pedido de Redis caído en el DLQ"

echo
echo "Resultado: $PASS ok, $FAIL fallidos"
[ "$FAIL" = 0 ]
