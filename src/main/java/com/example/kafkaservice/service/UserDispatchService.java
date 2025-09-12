package com.example.kafkaservice.service;

import com.example.kafkaservice.model.User;
import com.example.kafkaservice.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserDispatchService {

    private final UserRepository userRepository;
    private final KafkaProducerService kafkaProducerService;

    @Transactional
    public int dispatchUnsentUsers() {
        List<User> unsent = userRepository.findTop100ByIsSendFalseOrderByIdAsc();
        if (unsent.isEmpty()) {
            return 0;
        }

        for (User user : unsent) {
            try {
                kafkaProducerService.sendUserMessage(user);
                user.setSend(true);
            } catch (Exception ex) {
                log.error("Failed to dispatch user {}: {}", user.getId(), ex.getMessage(), ex);
            }
        }

        userRepository.saveAll(unsent);
        return unsent.size();
    }
}
