package com.smith.processor.listener;

import com.smith.processor.dto.EventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EventListener {

    private static final Logger logger = LoggerFactory.getLogger(EventListener.class);

    @KafkaListener(topics = "smith.events.ingestion.v1", groupId = "test")
    public void eventListener(EventDTO event){
        logger.info("message received: {}", event);
    }
}
