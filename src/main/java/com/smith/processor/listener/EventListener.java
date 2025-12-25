package com.smith.processor.listener;

import com.smith.processor.dto.EventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
public class EventListener {

    private static final Logger logger = LoggerFactory.getLogger(EventListener.class);


    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000,multiplier = 2.0),
            autoCreateTopics = "true",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            kafkaTemplate = "retryKafkaTemplate"
    )
    @KafkaListener(topics = "smith.events.ingestion.v1", groupId = "test")
    public void eventListener(EventDTO event){

        logger.info("message received: {}", event);

        if(!event.getSeverity().equals("test")){
            throw new RuntimeException();
        }
        logger.info("message correctly processed: {}", event);

    }

    @KafkaListener(topics = "smith.events.ingestion.v1-dlt", groupId = "test")
    public void eventListenerDlt(EventDTO event,
                                 @Header(value = KafkaHeaders.EXCEPTION_STACKTRACE, required = false) String exceptionMessage,
                                 @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                 @Header(KafkaHeaders.OFFSET) long offset) {

        logger.info("message received in dlt listener: event: {},topic: {},offset: {}, error: {}", event, topic, offset, exceptionMessage );
    }
}
