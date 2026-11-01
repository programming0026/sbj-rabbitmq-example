package com.juniverse.broker.controller;

import java.time.LocalDateTime;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.juniverse.broker.model.EventMessage;
import com.juniverse.broker.producer.FanoutExchangeProducer;

import lombok.RequiredArgsConstructor;

/**
 * Controlador REST para probar el exchange fanout
 *
 * El exchange fanout difunde los mensajes a TODAS las colas enlazadas.
 * La clave de enrutamiento se IGNORA por completo.
 *
 * Los 3 servicios de notificación recibirán cada mensaje:
 * - Email Notification Service (queue 1)
 * - SMS Notification Service (queue 2)
 * - Push Notification Service (queue 3)
 */
@RestController
@RequestMapping("/api/fanout")
@RequiredArgsConstructor
public class FanoutExchangeController {

    private final FanoutExchangeProducer fanoutProducer;

    /**
     * Difunde una notificación a TODOS los suscriptores
     * Las 3 colas recibirán este mensaje al mismo tiempo
     */
    @PostMapping("/broadcast")
    public ResponseEntity<String> broadcastNotification(@RequestBody(required = false) String content) {
        EventMessage message = EventMessage.builder()
                .id(UUID.randomUUID().toString())
                .type("broadcast")
                .content(content != null ? content : "Important system announcement!")
                .timestamp(LocalDateTime.now())
                .source("broadcast-service")
                .build();

        fanoutProducer.broadcastMessage(message);

        return ResponseEntity.ok("""
                Broadcast sent to ALL subscribers!
                Receivers:
                - Email Notification Service (queue 1)
                - SMS Notification Service (queue 2)
                - Push Notification Service (queue 3)
                """);
    }

    /**
     * Difunde una alerta del sistema
     */
    @PostMapping("/alert")
    public ResponseEntity<String> broadcastAlert(@RequestBody(required = false) String content) {
        EventMessage message = EventMessage.builder()
                .id(UUID.randomUUID().toString())
                .type("system-alert")
                .content(content != null ? content : "ALERT: System maintenance scheduled")
                .timestamp(LocalDateTime.now())
                .source("alert-service")
                .build();

        fanoutProducer.broadcastMessage(message);

        return ResponseEntity.ok("System alert broadcasted to ALL notification services!");
    }

    /**
     * Difunde un mensaje promocional
     */
    @PostMapping("/promo")
    public ResponseEntity<String> broadcastPromo(@RequestBody(required = false) String content) {
        EventMessage message = EventMessage.builder()
                .id(UUID.randomUUID().toString())
                .type("promotional")
                .content(content != null ? content : "Special offer: 50% off today only!")
                .timestamp(LocalDateTime.now())
                .source("marketing-service")
                .build();

        fanoutProducer.broadcastMessage(message);

        return ResponseEntity.ok("Promotional message broadcasted via Email, SMS, and Push!");
    }
}
