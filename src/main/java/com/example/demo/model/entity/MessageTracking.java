package com.example.demo.model.entity;

import com.example.demo.enums.MessagePriority;
import com.example.demo.enums.MessageStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "message_tracking",
        indexes = {
                @Index(name = "idx_tracking_id", columnList = "tracking_id"),
                @Index(name = "idx_status", columnList = "status"),
                @Index(name = "idx_created_at", columnList = "created_at")
        })
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageTracking {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "tracking_id", unique = true, nullable = false, length = 50)
    private String trackingId;

    @Column(name = "payload", columnDefinition = "NVARCHAR(MAX)", nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "priority", length = 20, nullable = false)
    private MessagePriority priority;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 20, nullable = false)
    private MessageStatus status;

    @Column(name = "queue_name", length = 50)
    private String queueName;

    @Column(name = "retry_count")
    private Integer retryCount = 0;

    @Column(name = "error_message", columnDefinition = "NVARCHAR(MAX)")
    private String errorMessage;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
