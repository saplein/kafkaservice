package com.example.kafkaservice.service;

import com.example.kafkaservice.model.KafkaMessage;
import com.example.kafkaservice.repository.KafkaMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final KafkaMessageRepository kafkaMessageRepository;

    @KafkaListener(
            topics = "${kafka.topics.input}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional
    public void consumeKafkaMessage(String raw) {
        log.info("Received payload: {}", raw);

        if (raw == null || raw.trim().isEmpty()) {
            log.warn("Received null or empty message, skipping");
            return;
        }

        try {
            saveKafkaMessage(raw);
            log.info("Message saved to database successfully");
        } catch (Exception e) {
            log.error("Error processing message: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void saveKafkaMessage(String message) {
        KafkaMessage entity = new KafkaMessage();
        entity.setMessage(message);
        kafkaMessageRepository.save(entity);
        log.debug("Saved message to kafkamessage table");
    }
}

