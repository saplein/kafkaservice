package com.example.kafkaservice.dto;

import com.example.kafkaservice.model.User;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import java.time.LocalDateTime;

@Data
public class KafkaMessageDto {
    private Long userId;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    private LocalDateTime timestamp;
    private Payload payload;

    @Data
    public static class Payload {
        private String firstName;
        private String lastName;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
        private LocalDateTime createdAt;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
        private LocalDateTime updatedAt;
    }

    public KafkaMessageDto(User user) {
        this.userId = user.getId();
        this.timestamp = LocalDateTime.now();
        this.payload = new Payload();
        this.payload.setFirstName(user.getFirstName());
        this.payload.setLastName(user.getLastName());
        this.payload.setCreatedAt(user.getCreatedAt());
        this.payload.setUpdatedAt(user.getUpdatedAt());
    }
}
