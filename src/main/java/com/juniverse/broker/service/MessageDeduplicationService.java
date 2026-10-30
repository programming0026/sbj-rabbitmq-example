package com.juniverse.broker.service;

import java.time.LocalDateTime;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.juniverse.broker.entity.ProcessedMessage;
import com.juniverse.broker.repository.ProcessedMessageRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for message deduplication to ensure idempotent processing.
 *
 * Stores processed message IDs in database to detect duplicates.
 * Includes automatic cleanup of old records.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class MessageDeduplicationService {

    private final ProcessedMessageRepository repository;

    private static final int RETENTION_DAYS = 7;

    /**
     * Check if message was already processed and mark it as processing.
     * Uses database-level locking to handle concurrent access.
     *
     * @param messageId Unique message identifier
     * @param queueName Queue where the message was received
     * @param messageType Type/category of the message
     * @return true if message is NEW and should be processed, false if DUPLICATE
     */
    @Transactional
    public boolean tryProcess(String messageId, String queueName, String messageType) {
        if (messageId == null || messageId.isBlank()) {
            log.warn("Message ID is null or empty - processing without deduplication");
            return true;
        }

        if (repository.existsByMessageIdAndQueueName(messageId, queueName)) {
            log.info("DUPLICATE detected - messageId: {}, queue: {}", messageId, queueName);
            return false;
        }

        ProcessedMessage processed = ProcessedMessage.builder()
                .messageId(messageId)
                .queueName(queueName)
                .processedAt(LocalDateTime.now())
                .status("PROCESSED")
                .messageType(messageType)
                .build();

        try {
            repository.save(processed);
        } catch (DataIntegrityViolationException ex) {
            // Otro consumidor lo registró entre la comprobación y el guardado: es un duplicado
            log.info("DUPLICATE detected (concurrent) - messageId: {}, queue: {}", messageId, queueName);
            return false;
        }
        log.debug("Message marked as processed - messageId: {}, queue: {}", messageId, queueName);
        return true;
    }

    /**
     * Simple check if message was already processed (without marking).
     */
    @Transactional(readOnly = true)
    public boolean isDuplicate(String messageId, String queueName) {
        return repository.existsByMessageIdAndQueueName(messageId, queueName);
    }

    /**
     * Simple check if message was already processed in any queue.
     */
    @Transactional(readOnly = true)
    public boolean isDuplicate(String messageId) {
        return repository.existsByMessageId(messageId);
    }

    /**
     * Mark message as failed (for retry tracking).
     */
    @Transactional
    public void markAsFailed(String messageId, String queueName, String messageType) {
        // Se actualiza el registro existente (si lo hay) en lugar de insertar uno repetido
        ProcessedMessage processed = repository.findByMessageIdAndQueueName(messageId, queueName)
                .orElseGet(() -> ProcessedMessage.builder().messageId(messageId).queueName(queueName).build());
        processed.setProcessedAt(LocalDateTime.now());
        processed.setStatus("FAILED");
        processed.setMessageType(messageType);
        repository.save(processed);
    }

    /**
     * Remove a message from processed list (to allow reprocessing).
     */
    @Transactional
    public void allowReprocess(String messageId) {
        repository.deleteByMessageId(messageId);
        log.info("Message removed from deduplication (all queues) - messageId: {}", messageId);
    }

    /**
     * Permite reprocesar un mensaje solo en una cola (las demás colas conservan su registro).
     *
     * @param messageId identificador del mensaje
     * @param queueName cola en la que se permite reprocesarlo
     */
    @Transactional
    public void allowReprocess(String messageId, String queueName) {
        repository.deleteByMessageIdAndQueueName(messageId, queueName);
        log.info("Message removed from deduplication - messageId: {}, queue: {}", messageId, queueName);
    }

    /**
     * Scheduled cleanup of old processed messages.
     * Runs daily at midnight.
     */
    @Scheduled(cron = "0 0 0 * * ?")
    @Transactional
    public void cleanupExpiredMessages() {
        LocalDateTime expirationDate = LocalDateTime.now().minusDays(RETENTION_DAYS);
        int deleted = repository.deleteExpiredMessages(expirationDate);
        log.info("Cleaned up {} expired message records (older than {} days)", deleted, RETENTION_DAYS);
    }

    /**
     * Manual cleanup (for testing or maintenance).
     */
    @Transactional
    public int cleanupOlderThan(int days) {
        LocalDateTime expirationDate = LocalDateTime.now().minusDays(days);
        int deleted = repository.deleteExpiredMessages(expirationDate);
        log.info("Manual cleanup: deleted {} records older than {} days", deleted, days);
        return deleted;
    }

    /**
     * Get count of processed messages for a specific queue.
     */
    @Transactional(readOnly = true)
    public long getProcessedCount(String queueName) {
        return repository.countByQueueName(queueName);
    }
}
