package com.juniverse.broker.repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.juniverse.broker.entity.ProcessedMessage;

@Repository
public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, Long> {

    /** Indica si el mensaje se procesó en alguna cola. */
    boolean existsByMessageId(String messageId);

    /** Registros de un mensaje (uno por cola). */
    List<ProcessedMessage> findByMessageId(String messageId);

    /** Registro de un mensaje en una cola concreta. */
    Optional<ProcessedMessage> findByMessageIdAndQueueName(String messageId, String queueName);

    /**
     * Borra los registros de un mensaje en todas las colas.
     * @return registros borrados
     */
    @Modifying
    @Query("DELETE FROM ProcessedMessage p WHERE p.messageId = :messageId")
    int deleteByMessageId(@Param("messageId") String messageId);

    /**
     * Borra el registro de un mensaje en una cola.
     * @return registros borrados
     */
    @Modifying
    @Query("DELETE FROM ProcessedMessage p WHERE p.messageId = :messageId AND p.queueName = :queueName")
    int deleteByMessageIdAndQueueName(@Param("messageId") String messageId, @Param("queueName") String queueName);

    /** Indica si el mensaje ya se procesó en esa cola. */
    boolean existsByMessageIdAndQueueName(String messageId, String queueName);

    /** Mensajes procesados por una cola. */
    List<ProcessedMessage> findByQueueName(String queueName);

    /**
     * Borra los registros anteriores a la fecha indicada.
     * @param expirationDate fecha límite
     * @return registros borrados
     */
    @Modifying
    @Query("DELETE FROM ProcessedMessage p WHERE p.processedAt < :expirationDate")
    int deleteExpiredMessages(@Param("expirationDate") LocalDateTime expirationDate);

    /** Cantidad de mensajes procesados por una cola. */
    long countByQueueName(String queueName);
}
