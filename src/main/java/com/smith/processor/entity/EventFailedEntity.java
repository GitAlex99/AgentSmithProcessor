package com.smith.processor.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.sql.Timestamp;
import java.util.UUID;

@Data
@Entity
@Table(name = "failed_events")
public class EventFailedEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    private String topic;
    private int kafka_partition;
    private long kafka_offset;
    private UUID id_event;
    private String payload;
    private String exception_type;
    private String exception_message;
    private String stacktrace;
    private String consumer_group;
    private Integer retry_count;
    private Timestamp created_at;
    private String status;
}
