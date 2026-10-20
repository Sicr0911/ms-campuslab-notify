package com.campuslab.notify.messaging;

import com.campuslab.notify.config.NotifyRabbitProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Consume q.cmd.prep: ticket de preparacion de sala/equipo para el
 * tecnico de laboratorio, generado cuando una reserva pasa a APROBADA.
 */
@Component
public class PreparationTicketListener {

    private static final Logger log = LoggerFactory.getLogger(PreparationTicketListener.class);

    private final ObjectMapper objectMapper;
    private final ProcessedEventTracker processedEventTracker;
    private final NotifyRabbitProperties.QueueDef cola;

    public PreparationTicketListener(ObjectMapper notifyObjectMapper, ProcessedEventTracker processedEventTracker,
                                     NotifyRabbitProperties props) {
        this.objectMapper = notifyObjectMapper;
        this.processedEventTracker = processedEventTracker;
        this.cola = props.queues().prep();
    }

    @RabbitListener(queues = "${campuslab.rabbitmq.queues.prep.name}")
    public void onMessage(Message message, Channel channel, @Header("amqp_deliveryTag") long deliveryTag) throws IOException {
        try {
            EventEnvelope envelope = objectMapper.readValue(message.getBody(), EventEnvelope.class);

            if (!processedEventTracker.marcarComoProcesadoSiEsNuevo(envelope.eventId())) {
                log.info("Evento {} ya procesado antes, se descarta (idempotencia).", envelope.eventId());
                channel.basicAck(deliveryTag, false);
                return;
            }

            log.info("Generando ticket de preparacion para el tecnico. type={} eventId={} traceId={} payload={}",
                    envelope.type(), envelope.eventId(), envelope.traceId(), envelope.payload());

            channel.basicAck(deliveryTag, false);
        } catch (Exception ex) {
            log.error("Fallo al procesar mensaje de {} (deliveryTag={}, redelivered={}); se rechaza sin reencolar "
                            + "y queda en la DLQ {}. Cuerpo: {}",
                    cola.name(), deliveryTag, message.getMessageProperties().getRedelivered(), cola.dlq(),
                    new String(message.getBody(), StandardCharsets.UTF_8), ex);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
