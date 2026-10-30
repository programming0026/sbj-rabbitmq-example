package com.juniverse.broker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Ejemplo de RabbitMQ con Spring Boot: exchanges fanout y topic, consumidores idempotentes y deduplicación en base de datos. */
@SpringBootApplication
@EnableScheduling
public class SbjRabbitmqExampleApplication {

	/**
	 * Arranca la aplicación.
	 * @param args argumentos de la línea de comandos
	 */
	public static void main(String[] args) {
		SpringApplication.run(SbjRabbitmqExampleApplication.class, args);
	}

}
