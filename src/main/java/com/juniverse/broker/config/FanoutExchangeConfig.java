package com.juniverse.broker.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuración del exchange fanout
 *
 * El exchange fanout difunde los mensajes a TODAS las colas enlazadas.
 * La clave de enrutamiento se IGNORA: todas las colas reciben el mensaje.
 *
 * Casos de uso:
 * - Difundir notificaciones a varios servicios
 * - Enviar actualizaciones a todos los suscriptores
 * - Distribución de eventos donde todos los consumidores necesitan los mismos datos
 */
@Configuration
public class FanoutExchangeConfig {

    @Value("${rabbitmq.fanout.exchange}")
    private String fanoutExchange;

    @Value("${rabbitmq.fanout.queue.notification1}")
    private String notificationQueue1;

    @Value("${rabbitmq.fanout.queue.notification2}")
    private String notificationQueue2;

    @Value("${rabbitmq.fanout.queue.notification3}")
    private String notificationQueue3;


    /** Exchange fanout: reparte cada mensaje a todas las colas enlazadas. */
    @Bean
    public FanoutExchange fanoutExchange() {
        return new FanoutExchange(fanoutExchange);
    }

    /** Cola de notificaciones por correo (duradera). */
    @Bean
    public Queue notificationQueue1() {
        return new Queue(notificationQueue1, true);
    }

    /** Cola de notificaciones por SMS (duradera). */
    @Bean
    public Queue notificationQueue2() {
        return new Queue(notificationQueue2, true);
    }

    /** Cola de notificaciones push (duradera). */
    @Bean
    public Queue notificationQueue3() {
        return new Queue(notificationQueue3, true);
    }

    /**
     * Enlaza la cola 1 con el exchange fanout.
     * @param notificationQueue1 cola
     * @param fanoutExchange exchange
     */
    @Bean
    public Binding fanoutBinding1(Queue notificationQueue1, FanoutExchange fanoutExchange) {
        return BindingBuilder.bind(notificationQueue1).to(fanoutExchange);
    }

    /**
     * Enlaza la cola 2 con el exchange fanout.
     * @param notificationQueue2 cola
     * @param fanoutExchange exchange
     */
    @Bean
    public Binding fanoutBinding2(Queue notificationQueue2, FanoutExchange fanoutExchange) {
        return BindingBuilder.bind(notificationQueue2).to(fanoutExchange);
    }

    /**
     * Enlaza la cola 3 con el exchange fanout.
     * @param notificationQueue3 cola
     * @param fanoutExchange exchange
     */
    @Bean
    public Binding fanoutBinding3(Queue notificationQueue3, FanoutExchange fanoutExchange) {
        return BindingBuilder.bind(notificationQueue3).to(fanoutExchange);
    }
}
