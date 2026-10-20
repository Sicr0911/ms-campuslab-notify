package com.campuslab.notify.documento;

import com.campuslab.notify.messaging.ProcessedEventTracker;
import com.campuslab.notify.support.Mensajes;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class VoucherGenerationListenerTest {

    private Channel channel;
    private VoucherGenerationListener listener;

    @BeforeEach
    void setUp() {
        channel = mock(Channel.class);
        listener = new VoucherGenerationListener(Mensajes.objectMapper(), new ProcessedEventTracker(), Mensajes.props());
    }

    @Test
    void conReservaId_generaElDocumentoYHaceAck() throws Exception {
        listener.onMessage(Mensajes.mensaje("""
                {"type":"RESERVA_EN_USO","eventId":"evt-v1","payload":{"reservaId":5}}
                """), channel, 1L);

        verify(channel).basicAck(1L, false);
    }

    @Test
    void sinReservaId_esNoRecuperable_vaALaDlq() throws Exception {
        listener.onMessage(Mensajes.mensaje("""
                {"type":"RESERVA_EN_USO","eventId":"evt-v2","payload":{}}
                """), channel, 2L);

        verify(channel).basicNack(2L, false, false);
    }
}
