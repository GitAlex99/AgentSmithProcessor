package com.smith.processor.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.smith.processor.model.EventType;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

@Data
public class EventDTO {
    private UUID id;
    private EventType type;
    private String source;
    private String severity;
    private JsonNode payload;
    private Instant timestamp;
    private UUID clientId;
    private Instant receivedAt;
    private String version;
}
