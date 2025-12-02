package com.example.demo.model.dto;

import com.example.demo.enums.MessagePriority;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessagePayload implements Serializable {

    private static final long serialVersionUID = 1L;

    private String trackingId;
    private String payload;
    private MessagePriority priority;
    private Integer retryCount;
    private LocalDateTime timestamp;
    private Map<String, Object> metadata;
}
