package com.campuslab.notify.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Nombres de exchanges, colas, routing keys y DLQs de ms-campuslab-notify,
 * centralizados en application.yml (prefijo campuslab.rabbitmq). Tanto
 * RabbitMqConfig (declaracion de beans) como los listeners leen desde aca,
 * asi que ningun nombre de recurso RabbitMQ queda hardcodeado en el codigo.
 */
@Validated
@ConfigurationProperties(prefix = "campuslab.rabbitmq")
public record NotifyRabbitProperties(
        @NotBlank String exchange,
        @Valid @NotNull TopicDef topicExchange,
        @NotBlank String deadLetterExchange,
        @Valid @NotNull Queues queues
) {

    /**
     * Exchange topic por donde llegan alertas internas (ej. stock agotado,
     * publicado por ms-campuslab-catalog con routing key email.stock-agotado).
     *
     * @param name                nombre del exchange (cmd.topic)
     * @param emailRoutingPattern patron que enruta esas alertas a la cola de email
     */
    public record TopicDef(
            @NotBlank String name,
            @NotBlank String emailRoutingPattern
    ) {
    }

    public record Queues(
            @Valid @NotNull QueueDef email,
            @Valid @NotNull QueueDef prep,
            @Valid @NotNull QueueDef voucher
    ) {
    }

    /**
     * @param name       cola principal (ej. q.cmd.email)
     * @param routingKey routing key con la que bookings publica en el exchange
     * @param dlq        cola de mensajes muertos asociada (ej. q.cmd.email.dlq)
     */
    public record QueueDef(
            @NotBlank String name,
            @NotBlank String routingKey,
            @NotBlank String dlq
    ) {
    }
}
