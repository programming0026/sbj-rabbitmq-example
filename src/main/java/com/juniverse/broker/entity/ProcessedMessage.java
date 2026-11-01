package com.juniverse.broker.entity;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.UniqueConstraint;
import jakarta.persistence.GenerationType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Entidad que registra los mensajes procesados para garantizar la idempotencia.
 * Evita procesar mensajes duplicados en los consumidores de RabbitMQ.
 */
@Entity
@Table(name = "processed_messages",
        uniqueConstraints = @UniqueConstraint(name = "uk_message_queue", columnNames = {"message_id", "queue_name"}),
        indexes = {
    @Index(name = "idx_processed_at", columnList = "processedAt"),
    @Index(name = "idx_queue_name", columnList = "queueName")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedMessage {

    /** Clave propia: un mismo mensaje puede procesarse en varias colas (una fila por cola). */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 100)
    private String messageId;

    @Column(nullable = false, length = 100)
    private String queueName;

    @Column(nullable = false)
    private LocalDateTime processedAt;

    @Column(length = 50)
    private String status;

    @Column(length = 500)
    private String messageType;
}
