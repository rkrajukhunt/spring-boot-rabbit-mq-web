package com.example.demo.model.dto;

import com.example.demo.enums.MessageStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageResponse {

    private String trackingId;
    private MessageStatus status;
    private String message;
    private LocalDateTime timestamp;
}
