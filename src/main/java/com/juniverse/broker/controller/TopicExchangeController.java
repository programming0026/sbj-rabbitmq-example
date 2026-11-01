package com.juniverse.broker.controller;

import java.time.LocalDateTime;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.juniverse.broker.model.EventMessage;
import com.juniverse.broker.producer.TopicExchangeProducer;

import lombok.RequiredArgsConstructor;

/**
 * Controlador REST para probar el exchange topic
 *
 * El exchange topic enruta los mensajes según patrones de la clave de enrutamiento:
 * - '*' coincide con exactamente una palabra
 * - '#' coincide con cero o más palabras
 *
 * Endpoints de prueba:
 * POST /api/topic/send/{routingKey}  - Envía con una clave de enrutamiento propia
 * POST /api/topic/order/created      - Coincide con la cola "order.*"
 * POST /api/topic/system/error       - Coincide con la cola "*.error"
 */
@RestController
@RequestMapping("/api/topic")
@RequiredArgsConstructor
public class TopicExchangeController {

    private final TopicExchangeProducer topicProducer;

    /**
     * Envía un mensaje con una clave de enrutamiento propia
     * Ejemplo: POST /api/topic/send/order.created
     */
    @PostMapping("/send/{routingKey}")
    public ResponseEntity<String> sendWithRoutingKey(
            @PathVariable String routingKey,
            @RequestBody(required = false) String content) {

        EventMessage message = EventMessage.builder()
                .id(UUID.randomUUID().toString())
                .type(routingKey)
                .content(content != null ? content : "Message for " + routingKey)
                .timestamp(LocalDateTime.now())
                .source("topic-controller")
                .build();

        topicProducer.sendMessage(routingKey, message);

        return ResponseEntity.ok("Message sent with routing key: " + routingKey +
                "Routing behavior:" +
                "- 'order.*' pattern -> ordersQueue" +
                "- '*.error' pattern -> errorsQueue" +
                "- '#' pattern -> allEventsQueue (catches all)");
    }

    /**
     * Envía evento de creación de orden.
     * Lo recibirán: ordersQueue (order.*) y allEventsQueue (#)
     */
    @PostMapping("/order/created")
    public ResponseEntity<String> sendOrderCreated(@RequestBody(required = false) String content) {
        EventMessage message = EventMessage.builder()
                .id(UUID.randomUUID().toString())
                .type("order.created")
                .content(content != null ? content : "New order has been created")
                .timestamp(LocalDateTime.now())
                .source("order-service")
                .build();

        topicProducer.sendOrderCreated(message);

        return ResponseEntity.ok("""
                Order created event sent!"
                "Routing key: order.created"
                "Received by: ordersQueue (order.*), allEventsQueue (#)
                """);
    }

    /**
     * Envía evento de actualización de orden.
     * Lo recibirán: ordersQueue (order.*) y allEventsQueue (#)
     */
    @PostMapping("/order/updated")
    public ResponseEntity<String> sendOrderUpdated(@RequestBody(required = false) String content) {
        EventMessage message = EventMessage.builder()
                .id(UUID.randomUUID().toString())
                .type("order.updated")
                .content(content != null ? content : "Order has been updated")
                .timestamp(LocalDateTime.now())
                .source("order-service")
                .build();

        topicProducer.sendOrderUpdated(message);

        return ResponseEntity.ok("""
                Order updated event sent!"
                "Routing key: order.updated"
                "Received by: ordersQueue (order.*), allEventsQueue (#)
                """);
    }

    /**
     * Envía el evento system.error
     * Lo recibirán: errorsQueue (*.error) y allEventsQueue (#)
     */
    @PostMapping("/system/error")
    public ResponseEntity<String> sendSystemError(@RequestBody(required = false) String content) {
        EventMessage message = EventMessage.builder()
                .id(UUID.randomUUID().toString())
                .type("system.error")
                .content(content != null ? content : "System error occurred!")
                .timestamp(LocalDateTime.now())
                .source("monitoring-service")
                .build();

        topicProducer.sendSystemError(message);

        return ResponseEntity.ok("""
                System error event sent!"
                "Routing key: system.error"
                "Received by: errorsQueue (*.error), allEventsQueue (#)
                """);
    }

    /**
     * Envía el evento payment.error
     * Lo recibirán: errorsQueue (*.error) y allEventsQueue (#)
     */
    @PostMapping("/payment/error")
    public ResponseEntity<String> sendPaymentError(@RequestBody(required = false) String content) {
        EventMessage message = EventMessage.builder()
                .id(UUID.randomUUID().toString())
                .type("payment.error")
                .content(content != null ? content : "Payment processing failed!")
                .timestamp(LocalDateTime.now())
                .source("payment-service")
                .build();

        topicProducer.sendPaymentError(message);

        return ResponseEntity.ok("""
                Payment error event sent!"
                "Routing key: payment.error"
                "Received by: errorsQueue (*.error), allEventsQueue (#)
                """);
    }

    /**
     * Envía el evento user.registered
     * Solo lo recibirá: allEventsQueue (#)
     * NO coincide con los patrones order.* ni *.error
     */
    @PostMapping("/user/registered")
    public ResponseEntity<String> sendUserRegistered(@RequestBody(required = false) String content) {
        EventMessage message = EventMessage.builder()
                .id(UUID.randomUUID().toString())
                .type("user.registered")
                .content(content != null ? content : "New user registered")
                .timestamp(LocalDateTime.now())
                .source("user-service")
                .build();

        topicProducer.sendUserRegistered(message);

        return ResponseEntity.ok("""
                User registered event sent!"
                "Routing key: user.registered"
                "Received by: allEventsQueue (#) ONLY"
                "Does NOT match: ordersQueue (order.*), errorsQueue (*.error)
                """);
    }
}
