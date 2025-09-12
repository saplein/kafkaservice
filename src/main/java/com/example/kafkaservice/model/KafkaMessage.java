package com.example.kafkaservice.model;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "kafka_message")
@Data
public class KafkaMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message", nullable = false)
    private String message;
}
