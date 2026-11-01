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
 * Servicio de deduplicación de mensajes para garantizar un procesamiento idempotente.
 *
 * Guarda en la base de datos los identificadores procesados para detectar duplicados.
 * Incluye la limpieza automática de registros antiguos.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DatabaseMessageDeduplicationService implements MessageDeduplicationService {

    private final ProcessedMessageRepository repository;

    private static final int RETENTION_DAYS = 7;

    /**
     * Comprueba si el mensaje ya se procesó y lo marca como en proceso.
     * Usa bloqueo a nivel de base de datos para manejar el acceso concurrente.
     *
     * @param messageId identificador único del mensaje
     * @param queueName cola donde se recibió el mensaje
     * @param messageType tipo o categoría del mensaje
     * @return {@code true} si el mensaje es NUEVO y debe procesarse; {@code false} si está DUPLICADO
     */
    @Transactional
    @Override
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
     * Comprobación simple de si el mensaje ya se procesó (sin marcarlo).
     */
    @Transactional(readOnly = true)
    @Override
    public boolean isDuplicate(String messageId, String queueName) {
        return repository.existsByMessageIdAndQueueName(messageId, queueName);
    }

    /**
     * Comprobación simple de si el mensaje ya se procesó en alguna cola.
     */
    @Transactional(readOnly = true)
    @Override
    public boolean isDuplicate(String messageId) {
        return repository.existsByMessageId(messageId);
    }

    /**
     * Marca el mensaje como fallido (para el seguimiento de reintentos).
     */
    @Transactional
    @Override
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
     * Quita un mensaje de la lista de procesados (para permitir reprocesarlo).
     */
    @Transactional
    @Override
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
    @Override
    public void allowReprocess(String messageId, String queueName) {
        repository.deleteByMessageIdAndQueueName(messageId, queueName);
        log.info("Message removed from deduplication - messageId: {}, queue: {}", messageId, queueName);
    }

    /**
     * Limpieza programada de mensajes procesados antiguos.
     * Se ejecuta a diario a medianoche.
     */
    @Scheduled(cron = "0 0 0 * * ?")
    @Transactional
    @Override
    public void cleanupExpiredMessages() {
        LocalDateTime expirationDate = LocalDateTime.now().minusDays(RETENTION_DAYS);
        int deleted = repository.deleteExpiredMessages(expirationDate);
        log.info("Cleaned up {} expired message records (older than {} days)", deleted, RETENTION_DAYS);
    }

    /**
     * Limpieza manual (para pruebas o mantenimiento).
     */
    @Transactional
    @Override
    public int cleanupOlderThan(int days) {
        LocalDateTime expirationDate = LocalDateTime.now().minusDays(days);
        int deleted = repository.deleteExpiredMessages(expirationDate);
        log.info("Manual cleanup: deleted {} records older than {} days", deleted, days);
        return deleted;
    }

    /**
     * Obtiene la cantidad de mensajes procesados de una cola.
     */
    @Transactional(readOnly = true)
    @Override
    public long getProcessedCount(String queueName) {
        return repository.countByQueueName(queueName);
    }
}
