package com.campuslab.notify.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declaracion centralizada de la topologia RabbitMQ que consume
 * ms-campuslab-notify. Todos los nombres salen de application.yml
 * (campuslab.rabbitmq.*, ver NotifyRabbitProperties).
 *
 * <pre>
 *   cmd.direct --email.send---> q.cmd.email   --(nack)--> cmd.dlx --> q.cmd.email.dlq
 *   cmd.direct --prep.ticket--> q.cmd.prep    --(nack)--> cmd.dlx --> q.cmd.prep.dlq
 *   cmd.direct --voucher.gen--> q.cmd.voucher --(nack)--> cmd.dlx --> q.cmd.voucher.dlq
 *   cmd.topic  --email.*-----> q.cmd.email   (alertas internas, ej. stock agotado de catalog)
 * </pre>
 *
 * <p>Cada cola principal lleva x-dead-letter-exchange y
 * x-dead-letter-routing-key: cuando un listener hace basicNack con
 * requeue=false, el broker reenruta el mensaje a su DLQ.</p>
 */
@Configuration
@EnableConfigurationProperties(NotifyRabbitProperties.class)
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

    /**
     * La topologia tambien la pre-declara infra/mq/definitions.json en el
     * broker. Si alguna propiedad no coincide (ej. distinto argumento de
     * DLX), RabbitMQ responde PRECONDITION_FAILED: con
     * ignoreDeclarationExceptions se registra un WARN en vez de impedir que
     * el servicio arranque y consuma de las colas ya existentes.
     */
    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin admin = new RabbitAdmin(connectionFactory);
        admin.setIgnoreDeclarationExceptions(true);
        return admin;
    }

    // ---------------------------------------------------------------- exchanges

    @Bean
    public DirectExchange commandExchange(NotifyRabbitProperties props) {
        return ExchangeBuilder.directExchange(props.exchange()).durable(true).build();
    }

    @Bean
    public TopicExchange alertTopicExchange(NotifyRabbitProperties props) {
        return ExchangeBuilder.topicExchange(props.topicExchange().name()).durable(true).build();
    }

    @Bean
    public DirectExchange deadLetterExchange(NotifyRabbitProperties props) {
        return ExchangeBuilder.directExchange(props.deadLetterExchange()).durable(true).build();
    }

    // ------------------------------------------------------ notificaciones email

    @Bean
    public Queue emailQueue(NotifyRabbitProperties props) {
        return colaConDlq(props, props.queues().email());
    }

    @Bean
    public Queue emailDlq(NotifyRabbitProperties props) {
        return QueueBuilder.durable(props.queues().email().dlq()).build();
    }

    @Bean
    public Binding emailBinding(Queue emailQueue, DirectExchange commandExchange, NotifyRabbitProperties props) {
        return BindingBuilder.bind(emailQueue).to(commandExchange).with(props.queues().email().routingKey());
    }

    /** Alertas internas (stock agotado de catalog) tambien terminan en q.cmd.email. */
    @Bean
    public Binding emailAlertBinding(Queue emailQueue, TopicExchange alertTopicExchange, NotifyRabbitProperties props) {
        return BindingBuilder.bind(emailQueue).to(alertTopicExchange).with(props.topicExchange().emailRoutingPattern());
    }

    @Bean
    public Binding emailDlqBinding(Queue emailDlq, DirectExchange deadLetterExchange, NotifyRabbitProperties props) {
        return BindingBuilder.bind(emailDlq).to(deadLetterExchange).with(props.queues().email().dlq());
    }

    // --------------------------------------------------- tickets de preparacion

    @Bean
    public Queue prepQueue(NotifyRabbitProperties props) {
        return colaConDlq(props, props.queues().prep());
    }

    @Bean
    public Queue prepDlq(NotifyRabbitProperties props) {
        return QueueBuilder.durable(props.queues().prep().dlq()).build();
    }

    @Bean
    public Binding prepBinding(Queue prepQueue, DirectExchange commandExchange, NotifyRabbitProperties props) {
        return BindingBuilder.bind(prepQueue).to(commandExchange).with(props.queues().prep().routingKey());
    }

    @Bean
    public Binding prepDlqBinding(Queue prepDlq, DirectExchange deadLetterExchange, NotifyRabbitProperties props) {
        return BindingBuilder.bind(prepDlq).to(deadLetterExchange).with(props.queues().prep().dlq());
    }

    // ------------------------------------------------------- vouchers / actas PDF

    @Bean
    public Queue voucherQueue(NotifyRabbitProperties props) {
        return colaConDlq(props, props.queues().voucher());
    }

    @Bean
    public Queue voucherDlq(NotifyRabbitProperties props) {
        return QueueBuilder.durable(props.queues().voucher().dlq()).build();
    }

    @Bean
    public Binding voucherBinding(Queue voucherQueue, DirectExchange commandExchange, NotifyRabbitProperties props) {
        return BindingBuilder.bind(voucherQueue).to(commandExchange).with(props.queues().voucher().routingKey());
    }

    @Bean
    public Binding voucherDlqBinding(Queue voucherDlq, DirectExchange deadLetterExchange, NotifyRabbitProperties props) {
        return BindingBuilder.bind(voucherDlq).to(deadLetterExchange).with(props.queues().voucher().dlq());
    }

    private Queue colaConDlq(NotifyRabbitProperties props, NotifyRabbitProperties.QueueDef def) {
        return QueueBuilder.durable(def.name())
                .deadLetterExchange(props.deadLetterExchange())
                .deadLetterRoutingKey(def.dlq())
                .build();
    }
}
