package com.campuslab.notify.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consume q.cmd.prep: ticket de preparacion de sala/equipo para el
 * tecnico de laboratorio, generado cuando una reserva pasa a APROBADA.
 */
@Component
public class PreparationTicketListener {

    private static final Logger log = LoggerFactory.getLogger(PreparationTicketListener.class);

    private final ObjectMapper objectMapper;
    private final ProcessedEventTracker processedEventTracker;

    public PreparationTicketListener(ObjectMapper notifyObjectMapper, ProcessedEventTracker processedEventTracker) {
        this.objectMapper = notifyObjectMapper;
        this.processedEventTracker = processedEventTracker;
    }

    @RabbitListener(queues = "q.cmd.prep")
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
            log.error("Error procesando mensaje de q.cmd.prep, se envia a la DLQ.", ex);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
