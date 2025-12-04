package com.example.demo.model.dto;

import com.example.demo.enums.MessagePriority;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MessageRequest {

    @NotBlank(message = "Payload cannot be blank")
    private String payload;

    // Optional - will be auto-assigned to MEDIUM if not provided
    private MessagePriority priority;

    private Map<String, String> metadata;
}
