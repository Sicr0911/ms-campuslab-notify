package com.campuslab.notify.messaging;

import com.campuslab.notify.support.Mensajes;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Verifica la politica de ACK/NACK de ConsumidorConAck con un consumidor de
 * prueba cuyo procesar() falla a voluntad.
 */
class ConsumidorConAckTest {

    private static final String EVENTO = """
            {"type":"RESERVA_APROBADA","eventId":"evt-42","payload":{"reservaId":5}}
            """;

    private Channel channel;
    private ProcessedEventTracker tracker;
    private RuntimeException falla;
    private AtomicInteger procesados;
    private ConsumidorConAck consumidor;

    @BeforeEach
    void setUp() {
        channel = mock(Channel.class);
        tracker = new ProcessedEventTracker();
        procesados = new AtomicInteger();
        falla = null;
        consumidor = new ConsumidorConAck(Mensajes.objectMapper(), tracker, Mensajes.props().queues().email()) {
            @Override
            protected void procesar(EventEnvelope envelope) {
                procesados.incrementAndGet();
                if (falla != null) {
                    throw falla;
                }
            }
        };
    }

    @Test
    void errorTransitorio_primerIntento_seReencolaParaReintentar() throws Exception {
        falla = new IllegalStateException("proveedor de email no responde");

        consumidor.consumir(Mensajes.mensaje(EVENTO, false), channel, 1L);

        verify(channel).basicNack(1L, false, true);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    void errorTransitorio_yaReentregado_vaALaDlqSinReintentosInfinitos() throws Exception {
        falla = new IllegalStateException("proveedor de email no responde");

        consumidor.consumir(Mensajes.mensaje(EVENTO, true), channel, 2L);

        verify(channel).basicNack(2L, false, false);
    }

    @Test
    void errorTransitorio_laReentregaSeProcesaYNoSeDescartaComoDuplicada() throws Exception {
        falla = new IllegalStateException("caida momentanea");
        consumidor.consumir(Mensajes.mensaje(EVENTO, false), channel, 1L);

        falla = null;
        consumidor.consumir(Mensajes.mensaje(EVENTO, true), channel, 2L);

        assertThat(procesados.get()).isEqualTo(2);
        verify(channel).basicAck(2L, false);
    }

    @Test
    void contenidoInvalidoDetectadoAlProcesar_vaDirectoALaDlq() throws Exception {
        falla = new MensajeInvalidoException("payload sin datos obligatorios");

        consumidor.consumir(Mensajes.mensaje(EVENTO, false), channel, 3L);

        verify(channel).basicNack(3L, false, false);
    }
}
