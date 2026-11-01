package com.juniverse.broker.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuración del exchange topic
 *
 * El exchange topic enruta los mensajes según patrones de la clave de enrutamiento:
 * - '*' coincide con exactamente una palabra
 * - '#' coincide con cero o más palabras
 *
 * Ejemplos de claves de enrutamiento:
 * - order.created -> coincide con «order.*» y «order.#»
 * - order.payment.completed -> coincide con «order.#» pero NO con «order.*»
 * - system.error -> coincide con «*.error» y «#.error»
 */
@Configuration
public class TopicExchangeConfig {

    @Value("${rabbitmq.topic.exchange}")
    private String topicExchange;

    @Value("${rabbitmq.topic.queue.orders}")
    private String ordersQueue;

    @Value("${rabbitmq.topic.queue.errors}")
    private String errorsQueue;

    @Value("${rabbitmq.topic.queue.all}")
    private String allQueue;

    /** Exchange de tipo topic: enruta por patrón de la clave de enrutamiento. */
    @Bean
    public TopicExchange topicExchange() {
        return new TopicExchange(topicExchange);
    }

    /** Cola de pedidos (duradera). */
    @Bean
    public Queue ordersQueue() {
        return new Queue(ordersQueue, true);
    }

    /** Cola de errores (duradera). */
    @Bean
    public Queue errorsQueue() {
        return new Queue(errorsQueue, true);
    }

    /** Cola que recibe todos los eventos (duradera). */
    @Bean
    public Queue allEventsQueue() {
        return new Queue(allQueue, true);
    }


    /**
     * Enlace: ordersQueue <- "order.*"
     * Coincide: order.created, order.updated, order.deleted
     * NO coincide: order.payment.completed (más de una palabra después de «order.»)
     */
    @Bean
    public Binding ordersBinding(Queue ordersQueue, TopicExchange topicExchange) {
        return BindingBuilder.bind(ordersQueue).to(topicExchange).with("order.*");
    }

    /**
     * Enlace: errorsQueue <- "*.error"
     * Coincide: system.error, payment.error, order.error
     * NO coincide: system.critical.error (más de una palabra antes de «.error»)
     */
    @Bean
    public Binding errorsBinding(Queue errorsQueue, TopicExchange topicExchange) {
        return BindingBuilder.bind(errorsQueue).to(topicExchange).with("*.error");
    }

    /**
     * Enlace: allEventsQueue <- "#"
     * Coincide con TODAS las claves de enrutamiento (patrón comodín)
     */
    @Bean
    public Binding allEventsBinding(Queue allEventsQueue, TopicExchange topicExchange) {
        return BindingBuilder.bind(allEventsQueue).to(topicExchange).with("#");
    }
}
