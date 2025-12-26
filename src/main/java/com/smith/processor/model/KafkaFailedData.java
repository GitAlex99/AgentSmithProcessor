package com.smith.processor.model;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class KafkaFailedData {
    private String topic;
    private int partition;
    private long offset;
    private String exception_type;
    private String exception_message;
    private String stackTrace;
    private String consumer_group;
    private Integer retry_count;
    private String status;
    private Timestamp failedAt;

}
