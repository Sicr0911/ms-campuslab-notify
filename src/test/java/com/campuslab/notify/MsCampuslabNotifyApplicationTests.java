package com.campuslab.notify;

import com.campuslab.notify.config.NotifyRabbitProperties;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Prueba de carga de contexto. Los listeners no arrancan (auto-startup=false),
 * asi que no se necesita un broker RabbitMQ real para ejecutarla.
 */
@SpringBootTest(properties = "spring.rabbitmq.listener.simple.auto-startup=false")
class MsCampuslabNotifyApplicationTests {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private NotifyRabbitProperties props;

    @Test
    void contextLoads() {
        assertThat(props.exchange()).isEqualTo("cmd.direct");
    }

    @Test
    void declaraColaPrincipalYDlqPorCadaCasoDeUso() {
        // email, prep y voucher: cola principal + DLQ por cada uno
        assertThat(context.getBeansOfType(Queue.class)).hasSize(6);
        // 3 hacia cmd.direct + 3 DLQ + alerta de stock (cmd.topic -> q.cmd.email)
        assertThat(context.getBeansOfType(Binding.class)).hasSize(7);

        Binding alerta = context.getBean("emailAlertBinding", Binding.class);
        assertThat(alerta.getExchange()).isEqualTo("cmd.topic");
        assertThat(alerta.getRoutingKey()).isEqualTo("email.*");

        Queue email = context.getBean("emailQueue", Queue.class);
        assertThat(email.getName()).isEqualTo("q.cmd.email");
        assertThat(email.getArguments())
                .containsEntry("x-dead-letter-exchange", "cmd.dlx")
                .containsEntry("x-dead-letter-routing-key", "q.cmd.email.dlq");
    }
}
