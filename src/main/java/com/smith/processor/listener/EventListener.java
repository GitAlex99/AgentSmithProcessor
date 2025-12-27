package com.smith.processor.listener;

import com.smith.processor.config.ProcessorMapper;
import com.smith.processor.dto.EventDTO;
import com.smith.processor.model.KafkaFailedData;
import com.smith.processor.service.EventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

        eventService.saveProcessedEvents(event,topic,offset,partition,"test");

        logger.info("message correctly processed with id: {}", event.getId());

    }

    @KafkaListener(topics = "smith.events.ingestion.v1-dlt", groupId = "test")
    public void eventListenerDlt(EventDTO event,
                                 @Header(value = KafkaHeaders.EXCEPTION_STACKTRACE, required = false) String strackTrace,
                                 @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                 @Header(KafkaHeaders.OFFSET) long offset,
                                 @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                 @Header(value = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false) String exceptionType,
                                 @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage) {

        logger.info("message received in dlt listener: event: {},topic: {},offset: {}, error: {}", event, topic, offset, exceptionMessage );

        KafkaFailedData failedData = ProcessorMapper.createFailedDataObj(topic,
                partition,
                offset,
                exceptionType,
                exceptionMessage,
                strackTrace,
                "test",
                3,
                "bh");

        eventService.saveEventFailed(event,failedData);
    }
}
