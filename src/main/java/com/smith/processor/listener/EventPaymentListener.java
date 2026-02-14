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
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class EventPaymentListener {
    private static final Logger logger = LoggerFactory.getLogger(EventLogListener.class);

    @Autowired
    private EventService eventService;

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000,multiplier = 2.0),
            autoCreateTopics = "true",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            kafkaTemplate = "retryKafkaTemplate"
    )
    @KafkaListener(
            topics = "smith.events.ingestion.v1.payment",
            groupId = "user-payment-processor",
            concurrency = "6"
    )
    public void eventListenerHighPayment(EventDTO event,
                                         @Header(KafkaHeaders.OFFSET) long offset,
                                         @Header(KafkaHeaders.RECEIVED_PARTITION) int partition){

        savePaymentEvent(event,offset,partition);

    }


    private void savePaymentEvent(EventDTO event, long offset, int partition){
        logger.info("message received in listener: {}", event);
        try {

            eventService.saveProcessedEvents(event, "smith.events.ingestion.v1.user.login", offset, partition, "test");

        } catch(Exception ex){

            saveFailedData(event,"smith.events.ingestion.v1.user.login",partition,offset,ex.getClass().getTypeName(),ex.getMessage(), Arrays.toString(ex.getStackTrace()),"test",3,"bo");

        }
        logger.info("message correctly processed with id: {}", event.getId());
    }
    private void saveFailedData(EventDTO event,String topic, int partition, long offset, String exceptionType, String exceptionMessage, String stackTrace, String consumerGroup,int retyCount, String status){

        logger.info("Event: {} failed to be saved, saving in failed event table", event.getId());

        KafkaFailedData failedData = ProcessorMapper.createFailedDataObj(topic,
                partition,
                offset,
                exceptionType,
                exceptionMessage,
                stackTrace,
                consumerGroup,
                retyCount,
                status);

        eventService.saveEventFailed(event,failedData);

        throw new RuntimeException();
    }
}
