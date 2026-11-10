# Orders Worker

Worker en **Java 21 + Spring Boot (WebFlux)** que consume pedidos de **Kafka**, los valida y enriquece
consultando una **API en Go**, y los guarda en **MongoDB**. Usa **Redis** para el lock distribuido y para
los mensajes fallidos con su contador de reintentos.

```
Kafka (orders) ──► OrderConsumer ──► OrderHandler ──► DistributedLock (Redis)
                                         │
                                         ▼
                                   OrderProcessor ──► EnrichmentClient ──► API Go (/customers, /products)
                                         │                 (timeout + retry exponencial + caché productos)
                                         ▼
                                      MongoDB (orders)

  fallo transitorio ──► FailedOrderStore (Redis: JSON + ZSET) ◄── RetryScheduler (reintentos con backoff)
  Redis caído / mensaje ilegible ──► DeadLetterPublisher ──► Kafka (orders-dlq)
```

## Estructura

| Carpeta | Contenido |
|---|---|
| `worker/` | Worker Java 21 (Spring Boot 3.4, WebFlux, reactor-kafka, Reactive Mongo/Redis) |
| `enrichment-api/` | API en Go (stdlib `net/http`) con catálogo de productos y clientes en memoria |
| `docker-compose.yml` | Kafka (KRaft), MongoDB, Redis, API Go y worker |

Paquetes del worker: `kafka` (consumo), `service` (proceso/orquestación), `client` (API externa),
`lock` (lock distribuido), `failure` (reintentos en Redis), `repository`, `model`, `config`.

## Cómo correrlo

```bash
docker compose up --build
```

Datos de prueba de la API Go: clientes `customer-456`, `customer-457` (activos) y `customer-999` (inactivo);
productos `product-789` (Laptop), `product-790`, `product-791`, `product-792`.

Enviar pedidos:

```bash
# válido
echo 'order-1:{"orderId":"order-1","customerId":"customer-456","products":[{"productId":"product-789","quantity":1},{"productId":"product-790","quantity":2}]}' \
 | docker exec -i orders-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders --property parse.key=true --property key.separator=:

# cliente inactivo -> REJECTED en Redis, no se guarda en Mongo
echo 'order-2:{"orderId":"order-2","customerId":"customer-999","products":[{"productId":"product-789","quantity":1}]}' \
 | docker exec -i orders-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders --property parse.key=true --property key.separator=:
```

Verificar:

```bash
docker exec orders-mongo mongosh orders --quiet --eval 'db.orders.find().pretty()'
docker exec orders-redis redis-cli --scan --pattern 'orders:failed:*'
```

Probar reintentos: `docker compose stop enrichment-api`, enviar un pedido y ver cómo queda `RETRYING` en Redis con
`attempts` creciente; al hacer `docker compose start enrichment-api` el `RetryScheduler` lo completa.

## Tests

```bash
cd worker && mvn test          # JUnit 5 + Mockito + reactor-test + MockWebServer
cd enrichment-api && go test ./...
```

### Test end to end

Con el stack levantado (`docker compose up --build -d`):

```bash
./scripts/e2e.sh        # COMPOSE="docker-compose" ./scripts/e2e.sh si usás el binario standalone
```

Verifica contra Kafka, Mongo, Redis y la API Go reales (17 comprobaciones, unos 3 minutos): pedido válido,
idempotencia, cliente inactivo, producto inexistente, mensaje ilegible al DLQ, API caída con reintentos y
recuperación, lock tomado por otra instancia, Redis caído con envío al DLQ y recuperación posterior.

## Decisiones de diseño

**Documento en Mongo** (colección `orders`): `orderId`, `customerId`, `products[{productId,name,price,...}]` como
pide el enunciado, más `customerName`, `quantity`, `total`, `processedAt`. `price` se guarda como `Decimal128`.

**Validación**: el cliente debe existir y estar activo, y todos los productos deben existir en el catálogo
(404 de la API Go → `OrderValidationException`). También se valida el mensaje (campos obligatorios, cantidad > 0).
Cliente y productos se consultan en paralelo.

**Reintentos exponenciales (dos niveles)**
1. *En la llamada HTTP*: `Retry.backoff` de Reactor (3 reintentos, 200 ms→2 s, jitter) solo para errores
   transitorios (timeout, conexión, 5xx, 429). Un 404 no se reintenta.
2. *Pedidos fallidos en Redis*: si la llamada igual falla, el pedido se guarda en `orders:failed:<orderId>`
   con `attempts`, último error y `nextRetryAt`; un ZSET (`orders:failed:retry-queue`) ordena por próximo
   intento. `RetryScheduler` lo reprocesa con backoff `base * 2^(n-1)` (5 s → 5 min). Al llegar a
   `max-attempts` (5) pasa a `DEAD`; los errores de validación quedan como `REJECTED` (sin reintento).
   Todo es configurable en `application.yml` (`worker.retry.*`).

**Lock distribuido**: `SET lock:order:<id> <token> NX PX <ttl>`; se libera con script Lua que verifica el token.
Si otra instancia tiene el lock, el pedido se omite (esa instancia ya lo procesa o lo reintentará).

**Consistencia**: consumo *at-least-once*: el offset se confirma después de manejar el pedido (éxito o fallo ya
persistido en Redis). Mongo tiene índice único en `orderId` y el procesamiento es idempotente
(si ya existe, se ignora el duplicado), por lo que redeliveries no generan duplicados.

**Rendimiento/escalabilidad**
- 100 % no bloqueante (WebFlux, reactor-kafka, drivers reactivos).
- Paralelismo entre particiones, orden garantizado dentro de cada partición; se escala horizontalmente
  agregando instancias (mismo `group-id`) y particiones.
- Caché Caffeine de productos (TTL 5 min); los clientes **no** se cachean porque `active` puede cambiar.
- Índices Mongo: único por `orderId` y compuesto `customerId + processedAt`.

**Cola de mensajes muertos (DLQ)**: topic `orders-dlq` (`worker.kafka.dlq-topic`), con los encabezados
`dlq-reason` y `dlq-at`. Recibe (1) los mensajes ilegibles y (2) los pedidos que fallaron cuando Redis tampoco
pudo registrar el fallo. El offset se confirma solo cuando el mensaje quedó a salvo (procesado, en Redis o en el
DLQ). Si ni Redis ni el DLQ responden, el error llega al consumidor: no confirma el offset y se re-suscribe con
espera creciente, y Kafka reentrega desde el último offset confirmado, sin perder pedidos.

Redis falla rápido: los comandos se rechazan al instante mientras no hay conexión y tienen timeout de 2 s
(`RedisConfig`). Sin eso, Lettuce los encola hasta reconectar y el consumidor quedaría colgado en vez de
derivar el pedido al DLQ. Mientras Redis se reconecta (unos segundos), los pedidos que llegan van al DLQ.

Para revisar el DLQ:

```bash
docker exec orders-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders-dlq --from-beginning --property print.headers=true
```

## Limitaciones / mejoras posibles

- Un mensaje en el DLQ no se reprocesa automáticamente; haría falta una herramienta o un consumidor aparte.
- Si un pedido en `RETRYING` vuelve a llegar por Kafka, el contador de intentos se reinicia en ese camino.
- Sin tests de integración con Testcontainers (requieren Docker); los tests actuales son unitarios.
- La API Go usa datos en memoria; en un caso real consultaría una base de datos.
