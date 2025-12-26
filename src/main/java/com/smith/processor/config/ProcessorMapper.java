package com.smith.processor.config;

import com.smith.processor.dto.EventDTO;
import com.smith.processor.entity.EventFailedEntity;
import com.smith.processor.model.KafkaFailedData;

import java.sql.Timestamp;
import java.time.Instant;

public class ProcessorMapper {

    public static KafkaFailedData createFailedDataObj(String topic, int partition, long offset, String exception_type, String exception_message, String stackTrace, String consumer_group, Integer retry_count, String status){

        KafkaFailedData failedData = new KafkaFailedData();
        failedData.setTopic(topic);
        failedData.setPartition(partition);
        failedData.setOffset(offset);
        failedData.setException_type(exception_type);
        failedData.setException_message(exception_message);
        failedData.setStackTrace(stackTrace);
        failedData.setConsumer_group(consumer_group);
        failedData.setRetry_count(retry_count);
        failedData.setStatus(status);
        failedData.setFailedAt(Timestamp.from(Instant.now()));

        return failedData;
    }

    public static EventFailedEntity toEntity(EventDTO dto, KafkaFailedData failedData){
        EventFailedEntity entity = new EventFailedEntity();
        entity.setTopic(failedData.getTopic());
        entity.setKafka_partition(failedData.getPartition());
        entity.setKafka_offset(failedData.getOffset());
        entity.setId_event(dto.getId());
        entity.setPayload(dto.getPayload().toString());
        entity.setException_type(failedData.getException_type());
        entity.setException_message(failedData.getException_message());
        entity.setStacktrace(failedData.getStackTrace().substring(0,1500));
        entity.setConsumer_group(failedData.getConsumer_group());
        entity.setRetry_count(failedData.getRetry_count());
        entity.setCreated_at(dto.getTimestamp());
        entity.setStatus(failedData.getStatus());

        return entity;
    }
}
