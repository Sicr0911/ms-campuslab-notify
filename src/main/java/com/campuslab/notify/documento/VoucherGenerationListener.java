package com.campuslab.notify.documento;

import com.campuslab.notify.config.NotifyRabbitProperties;
import com.campuslab.notify.messaging.ConsumidorConAck;
import com.campuslab.notify.messaging.EventEnvelope;
import com.campuslab.notify.messaging.MensajeInvalidoException;
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
 * Dominio DOCUMENTOS. Consume q.cmd.voucher: generacion del PDF de vale de
 * retiro (reserva EN_USO) o acta de devolucion. El ACK/NACK y el envio a la
 * DLQ los resuelve ConsumidorConAck.
 */
@Component
public class VoucherGenerationListener extends ConsumidorConAck {

    private static final Logger log = LoggerFactory.getLogger(VoucherGenerationListener.class);

    public VoucherGenerationListener(ObjectMapper notifyObjectMapper, ProcessedEventTracker processedEventTracker,
                                     NotifyRabbitProperties props) {
        super(notifyObjectMapper, processedEventTracker, props.queues().voucher());
    }

    @RabbitListener(queues = "${campuslab.rabbitmq.queues.voucher.name}")
    public void onMessage(Message message, Channel channel, @Header("amqp_deliveryTag") long deliveryTag) throws IOException {
        consumir(message, channel, deliveryTag);
    }

    @Override
    protected void procesar(EventEnvelope envelope) {
        // Sin reservaId no hay documento que generar: no tiene sentido reintentarlo.
        if (envelope.payload() == null || !envelope.payload().hasNonNull("reservaId")) {
            throw new MensajeInvalidoException("El evento " + envelope.eventId() + " no trae payload.reservaId");
        }
        log.info("Generando PDF (vale de retiro / acta de devolucion). type={} eventId={} reservaId={} traceId={}",
                envelope.type(), envelope.eventId(), envelope.payload().get("reservaId"), envelope.traceId());
    }
}
