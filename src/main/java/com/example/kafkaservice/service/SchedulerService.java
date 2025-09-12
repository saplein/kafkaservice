package com.example.kafkaservice.service;

import com.example.kafkaservice.repository.KafkaMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SchedulerService {

    private final KafkaMessageRepository kafkaMessageRepository;
    private final UserDispatchService userDispatchService;

    @Scheduled(fixedRate = 10000)
    public void dispatchUsers() {
        int dispatched = userDispatchService.dispatchUnsentUsers();
        if (dispatched > 0) {
            log.info("[SCHEDULER] Dispatched {} unsent users", dispatched);
        } else {
            log.debug("[SCHEDULER] No unsent users to dispatch");
        }
    }

    @Scheduled(cron = "0 * * * * *")
    public void reportSavedMessagesCount() {
        long count = kafkaMessageRepository.count();
        log.info("[SCHEDULER] Total saved Kafka messages: {}", count);
    }
}



