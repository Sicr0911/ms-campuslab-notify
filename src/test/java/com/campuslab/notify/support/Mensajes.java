package com.campuslab.notify.support;

import com.campuslab.notify.config.NotifyRabbitProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.nio.charset.StandardCharsets;

/** Datos de prueba compartidos por los tests de los consumidores. */
public final class Mensajes {

    private Mensajes() {
    }

    public static ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public static NotifyRabbitProperties props() {
        return new NotifyRabbitProperties("cmd.direct",
                new NotifyRabbitProperties.TopicDef("cmd.topic", "email.*"), "cmd.dlx",
                new NotifyRabbitProperties.Queues(
                        new NotifyRabbitProperties.QueueDef("q.cmd.email", "email.send", "q.cmd.email.dlq"),
                        new NotifyRabbitProperties.QueueDef("q.cmd.prep", "prep.ticket", "q.cmd.prep.dlq"),
                        new NotifyRabbitProperties.QueueDef("q.cmd.voucher", "voucher.gen", "q.cmd.voucher.dlq")));
    }

    public static Message mensaje(String body) {
        return mensaje(body, false);
    }

    public static Message mensaje(String body, boolean reentregado) {
        MessageProperties properties = new MessageProperties();
        properties.setRedelivered(reentregado);
        return new Message(body.getBytes(StandardCharsets.UTF_8), properties);
    }
}
