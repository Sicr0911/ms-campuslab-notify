package com.campuslab.notify.messaging;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deduplicacion de eventos por eventId, para que un redelivery de RabbitMQ
 * (ej. tras un NACK, un reinicio del consumer o una redistribucion del
 * cluster) no dispare la misma notificacion dos veces.
 *
 * <p>Almacenamiento en memoria: valido para el alcance de este caso
 * (ms-campuslab-notify no tiene base de datos propia), pero se pierde al
 * reiniciar el proceso y no se comparte entre replicas. En un entorno
 * productivo con mas de una instancia de este servicio, esto deberia
 * respaldarse en un store compartido (Redis, o una tabla en Oracle).</p>
 */
@Component
public class ProcessedEventTracker {

    private final Set<String> eventosProcesados = ConcurrentHashMap.newKeySet();

    /**
     * @return true si es la primera vez que se ve este eventId (y por lo
     * tanto corresponde procesarlo), false si ya se proceso antes.
     */
    public boolean marcarComoProcesadoSiEsNuevo(String eventId) {
        return eventosProcesados.add(eventId);
    }

    /**
     * Revierte la marca cuando el procesamiento fallo, para que una
     * reentrega de RabbitMQ no se descarte como duplicada.
     */
    public void olvidar(String eventId) {
        eventosProcesados.remove(eventId);
    }
}
