package com.campuslab.notify.messaging;

import com.campuslab.notify.config.NotifyRabbitProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class EmailNotificationListenerTest {

    private static final long DELIVERY_TAG = 7L;

    private Channel channel;
    private EmailNotificationListener listener;

    @BeforeEach
    void setUp() {
        NotifyRabbitProperties.QueueDef email =
                new NotifyRabbitProperties.QueueDef("q.cmd.email", "email.send", "q.cmd.email.dlq");
        NotifyRabbitProperties.QueueDef prep =
                new NotifyRabbitProperties.QueueDef("q.cmd.prep", "prep.ticket", "q.cmd.prep.dlq");
        NotifyRabbitProperties.QueueDef voucher =
                new NotifyRabbitProperties.QueueDef("q.cmd.voucher", "voucher.gen", "q.cmd.voucher.dlq");
        NotifyRabbitProperties props = new NotifyRabbitProperties("cmd.direct",
                new NotifyRabbitProperties.TopicDef("cmd.topic", "email.*"), "cmd.dlx",
                new NotifyRabbitProperties.Queues(email, prep, voucher));

        channel = mock(Channel.class);
        listener = new EmailNotificationListener(new ObjectMapper().registerModule(new JavaTimeModule()),
                new ProcessedEventTracker(), props);
    }

    @Test
    void mensajeValido_haceAckExplicito() throws Exception {
        listener.onMessage(mensaje("""
                {"type":"RESERVA_APROBADA","eventId":"evt-1","timestamp":"2026-10-19T10:00:00Z",
                 "traceId":"t-1","correlationId":"reserva-5","payload":{"reservaId":5}}
                """), channel, DELIVERY_TAG);

        verify(channel).basicAck(DELIVERY_TAG, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void alertaDeStockAgotadoDeCatalog_tambienSeConfirma() throws Exception {
        // mismo formato que publica CatalogEventPublisher (cmd.topic / email.stock-agotado)
        listener.onMessage(mensaje("""
                {"type":"STOCK_AGOTADO","eventId":"evt-stock-1","timestamp":"2026-10-19T10:00:00Z",
                 "traceId":"t-2","correlationId":"recurso-3",
                 "payload":{"recursoId":3,"nombre":"Osciloscopio","tipo":"EQUIPO"}}
                """), channel, DELIVERY_TAG);

        verify(channel).basicAck(DELIVERY_TAG, false);
    }

    @Test
    void eventoDuplicado_haceAckSinReprocesar() throws Exception {
        String json = """
                {"type":"RESERVA_APROBADA","eventId":"evt-dup","payload":{}}
                """;
        listener.onMessage(mensaje(json), channel, 1L);
        listener.onMessage(mensaje(json), channel, 2L);

        verify(channel).basicAck(1L, false);
        verify(channel).basicAck(2L, false);
    }

    @Test
    void mensajeInvalido_haceNackSinReencolarParaQueVayaALaDlq() throws Exception {
        listener.onMessage(mensaje("esto no es json"), channel, DELIVERY_TAG);

        verify(channel).basicNack(DELIVERY_TAG, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    private Message mensaje(String body) {
        return new Message(body.getBytes(StandardCharsets.UTF_8), new MessageProperties());
    }
}
