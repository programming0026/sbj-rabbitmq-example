package com.juniverse.broker.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.juniverse.broker.repository.ProcessedMessageRepository;
import com.juniverse.broker.service.MessageDeduplicationService;

import lombok.RequiredArgsConstructor;

/**
 * Controlador REST para monitorear y administrar la deduplicación de mensajes.
 */
@RestController
@RequestMapping("/api/deduplication")
@RequiredArgsConstructor
public class DeduplicationController {

    private final MessageDeduplicationService deduplicationService;
    private final ProcessedMessageRepository repository;

    /**
     * Obtiene estadísticas de los mensajes procesados
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("totalProcessedMessages", repository.count());
        stats.put("topicOrdersQueue", deduplicationService.getProcessedCount("topic.queue.orders"));
        stats.put("topicErrorsQueue", deduplicationService.getProcessedCount("topic.queue.errors"));
        stats.put("topicAllQueue", deduplicationService.getProcessedCount("topic.queue.all"));
        stats.put("fanoutNotification1", deduplicationService.getProcessedCount("fanout.queue.notification1"));
        stats.put("fanoutNotification2", deduplicationService.getProcessedCount("fanout.queue.notification2"));
        stats.put("fanoutNotification3", deduplicationService.getProcessedCount("fanout.queue.notification3"));

        return ResponseEntity.ok(stats);
    }

    /**
     * Obtiene todos los mensajes procesados
     */
    @GetMapping("/messages")
    public ResponseEntity<?> getAllProcessedMessages() {
        return ResponseEntity.ok(repository.findAll());
    }

    /**
     * Obtiene los mensajes procesados de una cola
     */
    @GetMapping("/messages/{queueName}")
    public ResponseEntity<?> getMessagesByQueue(@PathVariable String queueName) {
        return ResponseEntity.ok(repository.findByQueueName(queueName));
    }

    /**
     * Comprueba si un identificador de mensaje ya se procesó
     */
    @GetMapping("/check/{messageId}")
    public ResponseEntity<Map<String, Object>> checkMessage(@PathVariable String messageId) {
        Map<String, Object> result = new HashMap<>();
        result.put("messageId", messageId);
        result.put("isDuplicate", deduplicationService.isDuplicate(messageId));
        result.put("details", repository.findByMessageId(messageId)); // un registro por cola
        return ResponseEntity.ok(result);
    }

    /**
     * Permite reprocesar un mensaje concreto
     */
    @DeleteMapping("/messages/{messageId}")
    public ResponseEntity<String> allowReprocess(@PathVariable String messageId) {
        deduplicationService.allowReprocess(messageId);
        return ResponseEntity.ok("Message " + messageId + " can now be reprocessed");
    }

    /**
     * Limpieza manual de mensajes antiguos
     */
    @DeleteMapping("/cleanup")
    public ResponseEntity<Map<String, Object>> cleanup(@RequestParam(defaultValue = "7") int days) {
        int deleted = deduplicationService.cleanupOlderThan(days);
        Map<String, Object> result = new HashMap<>();
        result.put("deletedRecords", deleted);
        result.put("olderThanDays", days);
        return ResponseEntity.ok(result);
    }
}
