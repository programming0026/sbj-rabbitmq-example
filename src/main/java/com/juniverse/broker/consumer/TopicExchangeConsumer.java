package com.juniverse.broker.consumer;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.juniverse.broker.model.EventMessage;
import com.juniverse.broker.service.MessageDeduplicationService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Consumidor del exchange topic con procesamiento idempotente.
 *
 * Usa MessageDeduplicationService para evitar procesar mensajes duplicados.
 * Cada cola lleva su propio registro de mensajes procesados.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TopicExchangeConsumer {

    private final MessageDeduplicationService deduplicationService;

    private static final String ORDERS_QUEUE = "topic.queue.orders";
    private static final String ERRORS_QUEUE = "topic.queue.errors";
    private static final String ALL_QUEUE = "topic.queue.all";

    /**
     * Escucha la cola de pedidos: recibe los mensajes "order.*"
     * Ejemplos: order.created, order.updated, order.deleted
     */
    @RabbitListener(queues = "${rabbitmq.topic.queue.orders}")
    public void handleOrderEvents(EventMessage message) {
        log.info("=== ORDERS QUEUE ===");

        if (!deduplicationService.tryProcess(message.getId(), ORDERS_QUEUE, message.getType())) {
            log.warn("DUPLICATE order event ignored: {}", message.getId());
            return;
        }

        try {
            log.info("Processing order event: {}", message);
            log.info("Order details: type={}, content={}",
                    message.getType(), message.getContent());

            processOrder(message);

        } catch (Exception e) {
            log.error("Error processing order event: {}", e.getMessage());
            deduplicationService.allowReprocess(message.getId(), ORDERS_QUEUE);
            throw e;
        }
    }

    /**
     * Escucha la cola de errores: recibe los mensajes "*.error"
     * Ejemplos: system.error, payment.error, order.error
     */
    @RabbitListener(queues = "${rabbitmq.topic.queue.errors}")
    public void handleErrorEvents(EventMessage message) {
        log.info("=== ERRORS QUEUE ===");

        if (!deduplicationService.tryProcess(message.getId(), ERRORS_QUEUE, message.getType())) {
            log.warn("DUPLICATE error event ignored: {}", message.getId());
            return;
        }

        try {
            log.info("Processing error event: {}", message);
            log.warn("ERROR ALERT: type={}, content={}, source={}",
                    message.getType(), message.getContent(), message.getSource());


            handleError(message);

        } catch (Exception e) {
            log.error("Error handling error event: {}", e.getMessage());
            deduplicationService.allowReprocess(message.getId(), ERRORS_QUEUE);
            throw e;
        }
    }

    /**
     * Escucha la cola de todos los eventos: recibe TODOS los mensajes ("#" comodín)
     */
    @RabbitListener(queues = "${rabbitmq.topic.queue.all}")
    public void handleAllEvents(EventMessage message) {
        log.info("=== ALL EVENTS QUEUE ===");


        if (!deduplicationService.tryProcess(message.getId(), ALL_QUEUE, message.getType())) {
            log.warn("DUPLICATE event ignored in all-events queue: {}", message.getId());
            return;
        }

        try {
            log.info("Received event (catch-all): {}", message);


            auditEvent(message);

        } catch (Exception e) {
            log.error("Error in all-events handler: {}", e.getMessage());
            deduplicationService.allowReprocess(message.getId(), ALL_QUEUE);
            throw e;
        }
    }


    /**
     * Lógica de negocio de un pedido (simulada).
     * @param message evento recibido
     */
    private void processOrder(EventMessage message) {
        log.info("Processing order business logic for: {}", message.getType());
    }

    /**
     * Lógica para atender una alerta de error (simulada).
     * @param message evento recibido
     */
    private void handleError(EventMessage message) {
        log.info("Handling error alert for: {}", message.getSource());
    }

    /**
     * Registra el evento en la auditoría (simulado).
     * @param message evento recibido
     */
    private void auditEvent(EventMessage message) {
        log.info("Auditing event: {} from {}", message.getType(), message.getSource());
    }
}
