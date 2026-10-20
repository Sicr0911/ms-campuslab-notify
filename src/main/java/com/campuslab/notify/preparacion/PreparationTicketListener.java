package com.campuslab.notify.preparacion;

import com.campuslab.notify.config.NotifyRabbitProperties;
import com.campuslab.notify.messaging.ConsumidorConAck;
import com.campuslab.notify.messaging.EventEnvelope;
import com.campuslab.notify.messaging.ProcessedEventTracker;
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
 * Dominio PREPARACION DE LABORATORIO. Consume q.cmd.prep: ticket de
 * preparacion de sala/equipo para el tecnico, generado cuando una reserva
 * pasa a EN_PREPARACION. El ACK/NACK y el envio a la DLQ los resuelve
 * ConsumidorConAck.
 */
@Component
public class PreparationTicketListener extends ConsumidorConAck {

    private static final Logger log = LoggerFactory.getLogger(PreparationTicketListener.class);

    public PreparationTicketListener(ObjectMapper notifyObjectMapper, ProcessedEventTracker processedEventTracker,
                                     NotifyRabbitProperties props) {
        super(notifyObjectMapper, processedEventTracker, props.queues().prep());
    }

    @RabbitListener(queues = "${campuslab.rabbitmq.queues.prep.name}")
    public void onMessage(Message message, Channel channel, @Header("amqp_deliveryTag") long deliveryTag) throws IOException {
        consumir(message, channel, deliveryTag);
    }

    @Override
    protected void procesar(EventEnvelope envelope) {
        log.info("Generando ticket de preparacion para el tecnico. type={} eventId={} traceId={} payload={}",
                envelope.type(), envelope.eventId(), envelope.traceId(), envelope.payload());
    }
}
