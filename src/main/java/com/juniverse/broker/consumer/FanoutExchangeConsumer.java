package com.juniverse.broker.consumer;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.juniverse.broker.model.EventMessage;
import com.juniverse.broker.service.MessageDeduplicationService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Consumidor del exchange fanout con procesamiento idempotente.
 *
 * Cada suscriptor (cola) registra por separado los mensajes procesados.
 * El MISMO identificador de mensaje se procesará una vez por cola.
 *
 * Este es el comportamiento correcto en un fanout:
 * - El mensaje «ABC» llega a las 3 colas
 * - Cada cola procesa «ABC» exactamente UNA vez
 * - Se evitan los reintentos y duplicados dentro de cada cola
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class FanoutExchangeConsumer {

    private final MessageDeduplicationService deduplicationService;

    private static final String NOTIFICATION_QUEUE_1 = "fanout.queue.notification1";
    private static final String NOTIFICATION_QUEUE_2 = "fanout.queue.notification2";
    private static final String NOTIFICATION_QUEUE_3 = "fanout.queue.notification3";

    /**
     * Suscriptor 1: servicio de notificación por correo
     */
    @RabbitListener(queues = "${rabbitmq.fanout.queue.notification1}")
    public void emailNotificationHandler(EventMessage message) {
        log.info("=== EMAIL NOTIFICATION SERVICE ===");

        if (!deduplicationService.tryProcess(message.getId(), NOTIFICATION_QUEUE_1, message.getType())) {
            log.warn("DUPLICATE email notification ignored: {}", message.getId());
            return;
        }

        try {
            log.info("Sending email notification for: {}", message);
            sendEmail(message);
            log.info("Email sent successfully for message: {}", message.getId());

        } catch (Exception e) {
            log.error("Failed to send email: {}", e.getMessage());
            deduplicationService.allowReprocess(message.getId(), NOTIFICATION_QUEUE_1);
            throw e;
        }
    }

    /**
     * Suscriptor 2: servicio de notificación por SMS
     */
    @RabbitListener(queues = "${rabbitmq.fanout.queue.notification2}")
    public void smsNotificationHandler(EventMessage message) {
        log.info("=== SMS NOTIFICATION SERVICE ===");

        if (!deduplicationService.tryProcess(message.getId(), NOTIFICATION_QUEUE_2, message.getType())) {
            log.warn("DUPLICATE SMS notification ignored: {}", message.getId());
            return;
        }

        try {
            log.info("Sending SMS notification for: {}", message);
            sendSms(message);
            log.info("SMS sent successfully for message: {}", message.getId());

        } catch (Exception e) {
            log.error("Failed to send SMS: {}", e.getMessage());
            deduplicationService.allowReprocess(message.getId(), NOTIFICATION_QUEUE_2);
            throw e;
        }
    }

    /**
     * Suscriptor 3: servicio de notificación push
     */
    @RabbitListener(queues = "${rabbitmq.fanout.queue.notification3}")
    public void pushNotificationHandler(EventMessage message) {
        log.info("=== PUSH NOTIFICATION SERVICE ===");


        if (!deduplicationService.tryProcess(message.getId(), NOTIFICATION_QUEUE_3, message.getType())) {
            log.warn("DUPLICATE push notification ignored: {}", message.getId());
            return;
        }

        try {
            log.info("Sending push notification for: {}", message);
            sendPushNotification(message);
            log.info("Push notification sent successfully for message: {}", message.getId());

        } catch (Exception e) {
            log.error("Failed to send push notification: {}", e.getMessage());
            deduplicationService.allowReprocess(message.getId(), NOTIFICATION_QUEUE_3);
            throw e;
        }
    }


    /**
     * Simula el envío de un correo.
     * @param message evento a notificar
     */
    private void sendEmail(EventMessage message) {
        log.info("EMAIL -> To: users@example.com | Subject: {} | Body: {}",
                message.getType(), message.getContent());
    }

    /**
     * Simula el envío de un SMS.
     * @param message evento a notificar
     */
    private void sendSms(EventMessage message) {
        log.info("SMS -> To: +1234567890 | Message: {}", message.getContent());
    }

    /**
     * Simula el envío de una notificación push.
     * @param message evento a notificar
     */
    private void sendPushNotification(EventMessage message) {
        log.info("PUSH -> Title: {} | Body: {}", message.getType(), message.getContent());
    }
}
