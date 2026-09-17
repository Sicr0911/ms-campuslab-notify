package com.campuslab.notify.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqConfig {

    /**
     * ObjectMapper dedicado para deserializar el cuerpo JSON de los
     * mensajes (EventEnvelope, con un campo Instant). El ack/nack de cada
     * mensaje se maneja explicitamente en cada listener (ver
     * spring.rabbitmq.listener.simple.acknowledge-mode=manual en
     * application.yml), por lo que aqui no se registra un
     * Jackson2JsonMessageConverter automatico: cada listener recibe el
     * Message crudo y lo parsea con este bean.
     */
    @Bean
    public ObjectMapper notifyObjectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }
}
