package com.juniverse.broker.producer;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.juniverse.broker.model.EventMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Productor del exchange topic
 *
 * Envía mensajes con claves de enrutamiento que determinan
 * qué colas reciben el mensaje según la coincidencia de patrones.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TopicExchangeProducer {

    private final AmqpTemplate amqpTemplate;

    @Value("${rabbitmq.topic.exchange}")
    private String topicExchange;

    /**
     * Envía un mensaje con una clave de enrutamiento concreta
     *
     * @param routingKey clave de enrutamiento (por ejemplo «order.created», «system.error»)
     * @param message    mensaje que se envía
     */
    public void sendMessage(String routingKey, EventMessage message) {
        log.info("Sending to Topic Exchange [{}] with routing key [{}]: {}",
                topicExchange, routingKey, message);

        amqpTemplate.convertAndSend(topicExchange, routingKey, message);

        log.info("Message sent successfully with routing key: {}", routingKey);
    }

    /**
     * Envía con la clave {@code order.created}.
     * @param message evento
     */
    public void sendOrderCreated(EventMessage message) {
        sendMessage("order.created", message);
    }

    /**
     * Envía con la clave {@code order.updated}.
     * @param message evento
     */
    public void sendOrderUpdated(EventMessage message) {
        sendMessage("order.updated", message);
    }

    /**
     * Envía con la clave {@code system.error}.
     * @param message evento
     */
    public void sendSystemError(EventMessage message) {
        sendMessage("system.error", message);
    }

    /**
     * Envía con la clave {@code payment.error}.
     * @param message evento
     */
    public void sendPaymentError(EventMessage message) {
        sendMessage("payment.error", message);
    }

    /**
     * Envía con la clave {@code user.registered} (solo la recibe la cola de "todos los eventos").
     * @param message evento
     */
    public void sendUserRegistered(EventMessage message) {
        sendMessage("user.registered", message);
    }
}
