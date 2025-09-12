package com.example.kafkaservice.service;

import com.example.kafkaservice.model.KafkaMessage;
import com.example.kafkaservice.repository.KafkaMessageRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
    public void consumeKafkaMessage(Object raw) throws JsonProcessingException {
        log.info("Received payload of type {}: {}", raw == null ? "null" : raw.getClass().getName(), raw);

        if (raw == null) {
            log.warn("Received null or empty message, skipping");
            return;
        }

        try {
            String toSave;
            if (raw instanceof String s) {
                if (s.trim().isEmpty()) {
                    log.warn("Received empty string message, skipping");
                    return;
                }
                toSave = extractMessageFieldOrRaw(s);
            } else if (raw instanceof KafkaMessage kafkaMessage) {
                toSave = kafkaMessage.getMessage();
            } else if (raw instanceof ConsumerRecord<?, ?> record) {
                Object value = record.value();
                if (value == null) {
                    log.warn("ConsumerRecord has null value, skipping");
                    return;
                }
                if (value instanceof String sVal) {
                    if (sVal.trim().isEmpty()) {
                        log.warn("ConsumerRecord contains empty string value, skipping");
                        return;
                    }
                    toSave = extractMessageFieldOrRaw(sVal);
                } else if (value instanceof KafkaMessage kmVal) {
                    toSave = kmVal.getMessage();
                } else if (value instanceof byte[] bytesVal) {
                    String sVal = new String(bytesVal, java.nio.charset.StandardCharsets.UTF_8);
                    if (sVal.trim().isEmpty()) {
                        log.warn("ConsumerRecord contains empty bytes value, skipping");
                        return;
                    }
                    toSave = sVal;
                } else {
                    ObjectMapper mapper = new ObjectMapper();
                    String asJson = mapper.writeValueAsString(value);
                    toSave = extractMessageFieldOrRaw(asJson);
                }
            } else {
                ObjectMapper mapper = new ObjectMapper();
                String asJson = mapper.writeValueAsString(raw);
                toSave = extractMessageFieldOrRaw(asJson);
            }
            log.info("Extracted message to save: '{}'", toSave);

            if (toSave == null || toSave.trim().isEmpty()) {
                log.warn("Extracted message is null or empty, skipping save");
                return;
            }

            saveKafkaMessage(toSave);
            log.info("Message saved to database successfully");
        } catch (Exception e) {
            log.error("Error processing message: {}", e.getMessage(), e);
            throw e;
        }
    }

    private void saveKafkaMessage(String message) {
        KafkaMessage entity = new KafkaMessage();
        entity.setMessage(message);
        kafkaMessageRepository.save(entity);
        log.debug("Saved message to kafkamessage table");
    }

    private String extractMessageFieldOrRaw(String raw) {
        if (raw == null || raw.isBlank()) {
            return "";
        }
        try {
            ObjectMapper mapper = new ObjectMapper();
            var tree = mapper.readTree(raw);

            // Try direct 'message' field
            if (tree.hasNonNull("message")) {
                return tree.get("message").asText();
            }

            // Try nested 'payload.message'
            if (tree.has("payload") && tree.get("payload").hasNonNull("message")) {
                return tree.get("payload").get("message").asText();
            }

            // For KafkaMessageDto format: extract firstName + lastName
            if (tree.hasNonNull("firstName") || tree.hasNonNull("lastName")) {
                String firstName = tree.hasNonNull("firstName") ? tree.get("firstName").asText() : "";
                String lastName = tree.hasNonNull("lastName") ? tree.get("lastName").asText() : "";
                return String.format("%s %s", firstName, lastName).trim();
            }

            // Try nested 'payload.firstName' + 'payload.lastName'
            if (tree.has("payload")) {
                var payload = tree.get("payload");
                if (payload.hasNonNull("firstName") || payload.hasNonNull("lastName")) {
                    String firstName = payload.hasNonNull("firstName") ? payload.get("firstName").asText() : "";
                    String lastName = payload.hasNonNull("lastName") ? payload.get("lastName").asText() : "";
                    return String.format("%s %s", firstName, lastName).trim();
                }
            }

            // Fallback: store compacted JSON
            return mapper.writeValueAsString(tree);
        } catch (Exception ex) {
            // Not JSON, store as-is
            return raw;
        }
    }
}

