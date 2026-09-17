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
 * Consume q.cmd.voucher: generacion del PDF de vale de retiro (al aprobar
 * la reserva) o acta de devolucion (al registrar la devolucion del
 * equipo). El caso no asigna esta cola a otro microservicio, y por su
 * naturaleza (documento generado a partir de un evento de reserva, para
 * enviar o archivar) se agrupa junto a las demas notificaciones asincronas
 * de ms-campuslab-notify.
 */
@Component
public class VoucherGenerationListener {

    private static final Logger log = LoggerFactory.getLogger(VoucherGenerationListener.class);

    private final ObjectMapper objectMapper;
    private final ProcessedEventTracker processedEventTracker;

    public VoucherGenerationListener(ObjectMapper notifyObjectMapper, ProcessedEventTracker processedEventTracker) {
        this.objectMapper = notifyObjectMapper;
        this.processedEventTracker = processedEventTracker;
    }

    @RabbitListener(queues = "q.cmd.voucher")
    public void onMessage(Message message, Channel channel, @Header("amqp_deliveryTag") long deliveryTag) throws IOException {
        try {
            EventEnvelope envelope = objectMapper.readValue(message.getBody(), EventEnvelope.class);

            if (!processedEventTracker.marcarComoProcesadoSiEsNuevo(envelope.eventId())) {
                log.info("Evento {} ya procesado antes, se descarta (idempotencia).", envelope.eventId());
                channel.basicAck(deliveryTag, false);
                return;
            }

            log.info("Generando PDF (vale de retiro / acta de devolucion). type={} eventId={} traceId={} payload={}",
                    envelope.type(), envelope.eventId(), envelope.traceId(), envelope.payload());

            channel.basicAck(deliveryTag, false);
        } catch (Exception ex) {
            log.error("Error procesando mensaje de q.cmd.voucher, se envia a la DLQ.", ex);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
