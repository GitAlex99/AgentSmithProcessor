package com.smith.processor.entity;

import com.smith.processor.model.EventType;
import jakarta.persistence.*;
import lombok.Data;

import java.sql.Timestamp;
import java.util.UUID;

@Data
@Entity
@Table(name = "processed_events")
public class EventEntity {
    @Id
    private UUID id_event;
    private EventType type;
    private String source;
    private String severity;
    private String payload;
    private Timestamp sent_at;
    private UUID clientId;
    private Timestamp processed_at;
    private String version;
    private String topic;
    private long kafka_partition;
    private long kafka_offset;
    private String consumer_group;
    private String status;
}
