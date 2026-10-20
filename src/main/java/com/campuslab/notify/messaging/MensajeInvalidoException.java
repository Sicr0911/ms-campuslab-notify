package com.campuslab.notify.messaging;

/**
 * Error NO recuperable: el mensaje esta mal formado o le faltan datos
 * obligatorios. Reintentarlo daria siempre el mismo resultado, asi que va
 * directo a la DLQ (ver ConsumidorConAck).
 */
public class MensajeInvalidoException extends RuntimeException {

    public MensajeInvalidoException(String message) {
        super(message);
    }

    public MensajeInvalidoException(String message, Throwable cause) {
        super(message, cause);
    }
}
