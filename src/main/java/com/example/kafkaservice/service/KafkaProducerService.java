package com.example.kafkaservice.service;

import com.example.kafkaservice.dto.KafkaMessageDto;
import com.example.kafkaservice.model.User;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, KafkaMessageDto> kafkaTemplate;

    @Value("${user.write.service.topic}")
    private String kafkaTopic;

    public void sendUserMessage(User user) {
        try {
            KafkaMessageDto message = new KafkaMessageDto(user);

            kafkaTemplate.send(kafkaTopic, user.getId().toString(), message)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            System.out.println("Message sent to: " + kafkaTopic);
                        } else {
                            System.err.println("Failed to send: " + ex.getMessage());
                        }
                    });
        } catch (Exception e) {
            System.err.println("Kafka error: " + e.getMessage());
        }
    }
}
