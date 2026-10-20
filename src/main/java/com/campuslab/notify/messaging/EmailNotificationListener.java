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
 * Consume q.cmd.email: notificaciones al estudiante por email/push
 * (aprobacion de reserva, sala lista, devolucion registrada, etc.).
 *
 * <p>No hay integracion real con un proveedor de email/push en el alcance
 * de este caso: se simula el envio con un log. La responsabilidad real de
 * este listener es demostrar el patron de consumo async con ack/nack
 * explicitos e idempotencia por eventId, que es lo que exige el caso.</p>
 */
@Component
public class EmailNotificationListener {

    private static final Logger log = LoggerFactory.getLogger(EmailNotificationListener.class);

    private final ObjectMapper objectMapper;
    private final ProcessedEventTracker processedEventTracker;
    private final NotifyRabbitProperties.QueueDef cola;

    public EmailNotificationListener(ObjectMapper notifyObjectMapper, ProcessedEventTracker processedEventTracker,
                                     NotifyRabbitProperties props) {
        this.objectMapper = notifyObjectMapper;
        this.processedEventTracker = processedEventTracker;
        this.cola = props.queues().email();
    }

    @RabbitListener(queues = "${campuslab.rabbitmq.queues.email.name}")
    public void onMessage(Message message, Channel channel, @Header("amqp_deliveryTag") long deliveryTag) throws IOException {
        try {
            EventEnvelope envelope = objectMapper.readValue(message.getBody(), EventEnvelope.class);

            if (!processedEventTracker.marcarComoProcesadoSiEsNuevo(envelope.eventId())) {
                log.info("Evento {} ya procesado antes, se descarta (idempotencia).", envelope.eventId());
                channel.basicAck(deliveryTag, false);
                return;
            }

            log.info("Enviando email/push al estudiante. type={} eventId={} traceId={} payload={}",
                    envelope.type(), envelope.eventId(), envelope.traceId(), envelope.payload());

            channel.basicAck(deliveryTag, false);
        } catch (Exception ex) {
            // requeue=false: el broker reenruta el mensaje a q.cmd.email.dlq
            // (x-dead-letter-exchange declarado en RabbitMqConfig) en vez de
            // reintentarse infinitamente contra el mismo error.
            log.error("Fallo al procesar mensaje de {} (deliveryTag={}, redelivered={}); se rechaza sin reencolar "
                            + "y queda en la DLQ {}. Cuerpo: {}",
                    cola.name(), deliveryTag, message.getMessageProperties().getRedelivered(), cola.dlq(),
                    new String(message.getBody(), StandardCharsets.UTF_8), ex);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
