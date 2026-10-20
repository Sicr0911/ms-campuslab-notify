package com.campuslab.notify.notificacion;

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
 * Dominio NOTIFICACIONES. Consume q.cmd.email: notificaciones al estudiante
 * por email/push (aprobacion, cancelacion, devolucion de una reserva) y
 * alertas internas de stock agotado publicadas por catalog.
 *
 * <p>No hay integracion real con un proveedor de email/push en el alcance
 * de este caso: se simula el envio con un log. El ACK/NACK y el envio a la
 * DLQ los resuelve ConsumidorConAck.</p>
 */
@Component
public class EmailNotificationListener extends ConsumidorConAck {

    private static final Logger log = LoggerFactory.getLogger(EmailNotificationListener.class);

    public EmailNotificationListener(ObjectMapper notifyObjectMapper, ProcessedEventTracker processedEventTracker,
                                     NotifyRabbitProperties props) {
        super(notifyObjectMapper, processedEventTracker, props.queues().email());
    }

    @RabbitListener(queues = "${campuslab.rabbitmq.queues.email.name}")
    public void onMessage(Message message, Channel channel, @Header("amqp_deliveryTag") long deliveryTag) throws IOException {
        consumir(message, channel, deliveryTag);
    }

    @Override
    protected void procesar(EventEnvelope envelope) {
        log.info("Enviando email/push. type={} eventId={} traceId={} payload={}",
                envelope.type(), envelope.eventId(), envelope.traceId(), envelope.payload());
    }
}
