package com.campuslab.notify.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * Envelope comun que deben llevar todos los mensajes publicados en las
 * colas de comando (q.cmd.email, q.cmd.prep, q.cmd.voucher), segun la
 * buena practica descrita en el caso CampusLab: type, eventId, timestamp,
 * traceId, correlationId. "payload" es el contenido especifico de cada
 * tipo de evento (datos de la reserva, del destinatario, etc.).
 *
 * <p>Publicado por ms-campuslab-bookings (y eventualmente otros
 * productores) al exchange correspondiente; este microservicio solo lo
 * consume.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EventEnvelope(
        String type,
        String eventId,
        Instant timestamp,
        String traceId,
        String correlationId,
        JsonNode payload
) {
}
