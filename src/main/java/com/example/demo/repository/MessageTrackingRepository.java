package com.example.demo.repository;

import com.example.demo.enums.MessagePriority;
import com.example.demo.enums.MessageStatus;
import com.example.demo.model.entity.MessageTracking;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface MessageTrackingRepository extends JpaRepository<MessageTracking, Long> {

    Optional<MessageTracking> findByTrackingId(String trackingId);

    long countByStatus(MessageStatus status);

    List<MessageTracking> findByStatusAndCreatedAtBefore(
            MessageStatus status,
            LocalDateTime dateTime
    );

    @Query("SELECT m FROM MessageTracking m WHERE m.status = :status " +
            "AND m.priority = :priority ORDER BY m.createdAt DESC")
    List<MessageTracking> findByStatusAndPriority(
            @Param("status") MessageStatus status,
            @Param("priority") MessagePriority priority,
            Pageable pageable
    );

    // For bulk operations (cleanup old records)
    @Modifying
    @Query("DELETE FROM MessageTracking m WHERE m.status = :status " +
            "AND m.createdAt < :cutoffDate")
    int deleteOldRecordsByStatus(
            @Param("status") MessageStatus status,
            @Param("cutoffDate") LocalDateTime cutoffDate
    );

    // Processing duration metrics
    @Query("SELECT AVG(m.processingDurationMs) FROM MessageTracking m " +
            "WHERE m.processingDurationMs IS NOT NULL")
    Double getAverageProcessingDuration();

    @Query("SELECT AVG(m.processingDurationMs) FROM MessageTracking m " +
            "WHERE m.processingDurationMs IS NOT NULL AND m.createdAt >= :since")
    Double getAverageProcessingDurationSince(@Param("since") LocalDateTime since);

    @Query("SELECT COUNT(m) FROM MessageTracking m " +
            "WHERE m.processingDurationMs IS NOT NULL")
    long countProcessedMessages();
}
