package com.campuslab.notify.messaging;

import com.campuslab.notify.config.NotifyRabbitProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Politica comun de confirmacion de mensajes para todos los consumidores de
 * ms-campuslab-notify (acknowledge-mode: manual). Cada consumidor de dominio
 * (notificacion, preparacion, documento) solo implementa procesar(); el
 * ACK/NACK y el envio a la DLQ se deciden aca, de forma consistente:
 *
 * <table>
 *   <tr><th>Situacion</th><th>Decision</th></tr>
 *   <tr><td>Procesado OK</td><td>basicAck</td></tr>
 *   <tr><td>eventId ya procesado (redelivery)</td><td>basicAck sin reprocesar (idempotencia)</td></tr>
 *   <tr><td>Error NO recuperable: JSON invalido o faltan type/eventId
 *       (MensajeInvalidoException)</td><td>basicNack(requeue=false): directo a la DLQ</td></tr>
 *   <tr><td>Error recuperable (cualquier otra excepcion), primer intento</td>
 *       <td>basicNack(requeue=true): RabbitMQ lo reentrega una vez</td></tr>
 *   <tr><td>Error recuperable, mensaje ya reentregado (redelivered=true)</td>
 *       <td>basicNack(requeue=false): a la DLQ, sin reintentos infinitos</td></tr>
 * </table>
 *
 * <p>Todo rechazo hacia la DLQ queda registrado con log.error (cola, DLQ,
 * deliveryTag, motivo y cuerpo del mensaje).</p>
 */
public abstract class ConsumidorConAck {

    private static final Logger log = LoggerFactory.getLogger(ConsumidorConAck.class);

    private final ObjectMapper objectMapper;
    private final ProcessedEventTracker processedEventTracker;
    private final NotifyRabbitProperties.QueueDef cola;

    protected ConsumidorConAck(ObjectMapper objectMapper, ProcessedEventTracker processedEventTracker,
                               NotifyRabbitProperties.QueueDef cola) {
        this.objectMapper = objectMapper;
        this.processedEventTracker = processedEventTracker;
        this.cola = cola;
    }

    /**
     * Logica de negocio del consumidor. Lanzar MensajeInvalidoException si
     * el contenido no sirve (va a la DLQ); cualquier otra excepcion se trata
     * como transitoria y se reintenta una vez.
     */
    protected abstract void procesar(EventEnvelope envelope);

    protected final void consumir(Message message, Channel channel, long deliveryTag) throws IOException {
        EventEnvelope envelope;
        try {
            envelope = leer(message);
        } catch (MensajeInvalidoException ex) {
            enviarADlq(message, channel, deliveryTag, "mensaje invalido (no recuperable)", ex);
            return;
        }

        if (!processedEventTracker.marcarComoProcesadoSiEsNuevo(envelope.eventId())) {
            log.info("Evento {} de {} ya procesado antes, se confirma sin reprocesar (idempotencia).",
                    envelope.eventId(), cola.name());
            channel.basicAck(deliveryTag, false);
            return;
        }

        try {
            procesar(envelope);
            channel.basicAck(deliveryTag, false);
        } catch (MensajeInvalidoException ex) {
            processedEventTracker.olvidar(envelope.eventId());
            enviarADlq(message, channel, deliveryTag, "contenido invalido (no recuperable)", ex);
        } catch (Exception ex) {
            // Se "olvida" el eventId para que la reentrega no se descarte como duplicado.
            processedEventTracker.olvidar(envelope.eventId());
            if (Boolean.TRUE.equals(message.getMessageProperties().getRedelivered())) {
                enviarADlq(message, channel, deliveryTag, "error recuperable que persistio tras el reintento", ex);
            } else {
                log.warn("Error transitorio procesando evento {} de {} (deliveryTag={}); se reencola para un reintento.",
                        envelope.eventId(), cola.name(), deliveryTag, ex);
                channel.basicNack(deliveryTag, false, true);
            }
        }
    }

    private EventEnvelope leer(Message message) {
        EventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(message.getBody(), EventEnvelope.class);
        } catch (IOException ex) {
            throw new MensajeInvalidoException("El cuerpo no es un EventEnvelope JSON valido", ex);
        }
        if (envelope == null || !StringUtils.hasText(envelope.eventId()) || !StringUtils.hasText(envelope.type())) {
            throw new MensajeInvalidoException("Faltan campos obligatorios del envelope (type, eventId)");
        }
        return envelope;
    }

    private void enviarADlq(Message message, Channel channel, long deliveryTag, String motivo, Exception ex)
            throws IOException {
        log.error("Mensaje de {} rechazado hacia la DLQ {}: {} (deliveryTag={}, redelivered={}). Cuerpo: {}",
                cola.name(), cola.dlq(), motivo, deliveryTag, message.getMessageProperties().getRedelivered(),
                new String(message.getBody(), StandardCharsets.UTF_8), ex);
        channel.basicNack(deliveryTag, false, false);
    }
}
