package com.smith.processor.entity;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "technical_failure")
@Data
public class TechnicalFailureEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    private String topic;
    private int kafka_partition;
    private long kafka_offset;
    private String stacktrace_listener;
    private String raw_event;
}
