package com.smith.processor.listener;

import com.smith.processor.config.ProcessorMapper;
import com.smith.processor.dto.EventDTO;
import com.smith.processor.model.KafkaFailedData;
import com.smith.processor.service.EventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class EventListener {

    private static final Logger logger = LoggerFactory.getLogger(EventListener.class);

    @Autowired
    private EventService eventService;

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000,multiplier = 2.0),
            autoCreateTopics = "true",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            kafkaTemplate = "retryKafkaTemplate"
    )
    @KafkaListener(topics = "smith.events.ingestion.v1", groupId = "test")
    public void eventListener(EventDTO event,
                              @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @Header(KafkaHeaders.OFFSET) long offset,
                              @Header(KafkaHeaders.RECEIVED_PARTITION) int partition){

        logger.info("message received in listener: {}", event);
        try {
           eventService.saveProcessedEvents(event, topic, offset, partition, "test");
        } catch(Exception ex){
            logger.info("Event: {} failed to be saved, saving in failed event table", event.getId());

            KafkaFailedData failedData = ProcessorMapper.createFailedDataObj(topic,
                    partition,
                    offset,
                    ex.getClass().getTypeName(),
                    ex.getMessage(),
                    Arrays.toString(ex.getStackTrace()),
                    "test",
                    3,
                    "bh");

            eventService.saveEventFailed(event,failedData);

            throw new RuntimeException();
        }
        logger.info("message correctly processed with id: {}", event.getId());

    }

    @KafkaListener(topics = "smith.events.ingestion.v1-dlt", groupId = "test")
    public void eventListenerDlt(EventDTO event,
                                 @Header(value = KafkaHeaders.EXCEPTION_STACKTRACE, required = false) String strackTrace,
                                 @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                 @Header(KafkaHeaders.OFFSET) long offset,
                                 @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        logger.info("message received in dlt listener: event: {},topic: {},offset: {}", event, topic, offset);

        eventService.saveTechnicalFailure(event,topic,partition,offset,strackTrace);

        logger.info("eventListenerDlt END");
    }
}
