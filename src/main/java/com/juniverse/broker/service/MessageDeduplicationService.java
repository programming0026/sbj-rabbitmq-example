package com.juniverse.broker.service;

/**
 * Contrato de MessageDeduplicationService.
 */
public interface MessageDeduplicationService {

    /**
     * Comprueba si el mensaje ya se procesó y lo marca como en proceso.
     * Usa bloqueo a nivel de base de datos para manejar el acceso concurrente.
     *
     * @param messageId identificador único del mensaje
     * @param queueName cola donde se recibió el mensaje
     * @param messageType tipo o categoría del mensaje
     * @return {@code true} si el mensaje es NUEVO y debe procesarse; {@code false} si está DUPLICADO
     */
    boolean tryProcess(String messageId, String queueName, String messageType);

    /**
     * Comprobación simple de si el mensaje ya se procesó (sin marcarlo).
     */
    boolean isDuplicate(String messageId, String queueName);

    /**
     * Comprobación simple de si el mensaje ya se procesó en alguna cola.
     */
    boolean isDuplicate(String messageId);

    /**
     * Marca el mensaje como fallido (para el seguimiento de reintentos).
     */
    void markAsFailed(String messageId, String queueName, String messageType);

    /**
     * Quita un mensaje de la lista de procesados (para permitir reprocesarlo).
     */
    void allowReprocess(String messageId);

    /**
     * Permite reprocesar un mensaje solo en una cola (las demás colas conservan su registro).
     *
     * @param messageId identificador del mensaje
     * @param queueName cola en la que se permite reprocesarlo
     */
    void allowReprocess(String messageId, String queueName);

    /**
     * Limpieza programada de mensajes procesados antiguos.
     * Se ejecuta a diario a medianoche.
     */
    void cleanupExpiredMessages();

    /**
     * Limpieza manual (para pruebas o mantenimiento).
     */
    int cleanupOlderThan(int days);

    /**
     * Obtiene la cantidad de mensajes procesados de una cola.
     */
    long getProcessedCount(String queueName);
}
