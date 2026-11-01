package com.juniverse.broker.producer;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.juniverse.broker.model.EventMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Productor del exchange fanout
 *
 * Difunde los mensajes a TODAS las colas enlazadas.
 * La clave de enrutamiento se ignora: todas las colas reciben el mensaje.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class FanoutExchangeProducer {

    private final AmqpTemplate amqpTemplate;

    @Value("${rabbitmq.fanout.exchange}")
    private String fanoutExchange;

    /**
     * Difunde el mensaje a todas las colas enlazadas al exchange fanout
     *
     * @param message mensaje que se difunde
     */
    public void broadcastMessage(EventMessage message) {
        log.info("Broadcasting to Fanout Exchange [{}]: {}", fanoutExchange, message);

        amqpTemplate.convertAndSend(fanoutExchange, "", message);

        log.info("Message broadcasted to all subscribers");
    }
}
