package com.example.demo.model.dto;

import com.example.demo.enums.MessagePriority;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageRequest {

    @NotBlank(message = "Payload cannot be blank")
    private String payload;

    @NotNull(message = "Priority is required")
    private MessagePriority priority;

    private Map<String, Object> metadata;
}
