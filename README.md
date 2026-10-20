# ms-campuslab-notify

Consumidor RabbitMQ de CampusLab: procesa de forma asíncrona las
**notificaciones**, los **tickets de preparación** y la **generación de
documentos** que publican ms-campuslab-bookings y ms-campuslab-catalog. No
expone endpoints de negocio (solo `/actuator/health`).

## Topología

Todos los nombres están centralizados en `application.yml`
(`campuslab.rabbitmq.*`, leídos por `NotifyRabbitProperties`) y los beans de
`Queue`, `Exchange` y `Binding` se declaran en `config/RabbitMqConfig`.

```
cmd.direct --email.send---> q.cmd.email   --nack--> cmd.dlx --> q.cmd.email.dlq
cmd.direct --prep.ticket--> q.cmd.prep    --nack--> cmd.dlx --> q.cmd.prep.dlq
cmd.direct --voucher.gen--> q.cmd.voucher --nack--> cmd.dlx --> q.cmd.voucher.dlq
cmd.topic  --email.*------> q.cmd.email   (alerta de stock agotado de catalog)
```

## Consumidores por dominio

| Paquete | Consumidor | Cola | Responsabilidad |
|---|---|---|---|
| `notificacion` | `EmailNotificationListener` | `q.cmd.email` | Email/push al estudiante y alertas de stock |
| `preparacion` | `PreparationTicketListener` | `q.cmd.prep` | Ticket de preparación para el técnico |
| `documento` | `VoucherGenerationListener` | `q.cmd.voucher` | Vale de retiro / acta de devolución (PDF) |
| `messaging` | `ConsumidorConAck` (base común) | — | Política de ACK/NACK, DLQ, idempotencia |
| `config` | `RabbitMqConfig`, `NotifyRabbitProperties` | — | Configuración de mensajería (separada del negocio) |

Cada consumidor usa `@RabbitListener` y solo implementa `procesar()`; la
confirmación de mensajes la resuelve `ConsumidorConAck`.

## Confirmación de mensajes y manejo de errores

`acknowledge-mode: manual`: cada mensaje se confirma o rechaza
explícitamente.

| Situación | Tipo de error | Decisión | Resultado |
|---|---|---|---|
| Procesado correctamente | — | `basicAck` | Se elimina de la cola |
| `eventId` ya procesado (reentrega) | — | `basicAck` sin reprocesar | Idempotencia |
| JSON inválido o faltan `type`/`eventId` | **No recuperable** (`MensajeInvalidoException`) | `basicNack(requeue=false)` | Directo a la DLQ |
| Datos de negocio inválidos (ej. voucher sin `reservaId`) | **No recuperable** | `basicNack(requeue=false)` | Directo a la DLQ |
| Cualquier otra excepción, primer intento | **Recuperable** (transitorio) | `basicNack(requeue=true)` | RabbitMQ lo reentrega una vez |
| Error recuperable con `redelivered=true` | Recuperable que persiste | `basicNack(requeue=false)` | A la DLQ, sin reintentos infinitos |

Todo rechazo hacia la DLQ queda registrado con `log.error`, incluyendo la
cola, la DLQ de destino, el motivo, el `deliveryTag` y el cuerpo del mensaje.
Los reintentos quedan registrados con `log.warn`.

## Pruebas

```bash
mvn test
```

- `MsCampuslabNotifyApplicationTests`: carga de contexto y beans de la topología (sin broker).
- `ConsumidorConAckTest`: cada rama de la política de ACK/NACK.
- `EmailNotificationListenerTest`, `VoucherGenerationListenerTest`: consumidores de dominio.
