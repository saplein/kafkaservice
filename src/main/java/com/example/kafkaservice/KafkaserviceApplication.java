package com.example.kafkaservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaserviceApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaserviceApplication.class, args);
    }

}
