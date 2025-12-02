-- RabbitMQ High-Throughput Messaging System
-- Database Setup Script for MSSQL

-- Create Database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'message_tracking_db')
BEGIN
    CREATE DATABASE message_tracking_db;
END
GO

USE message_tracking_db;
GO

-- Create message_tracking table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[message_tracking]') AND type in (N'U'))
BEGIN
    CREATE TABLE message_tracking (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        tracking_id VARCHAR(50) UNIQUE NOT NULL,
        payload NVARCHAR(MAX) NOT NULL,
        priority VARCHAR(20) NOT NULL,
        status VARCHAR(20) NOT NULL,
        queue_name VARCHAR(50) NULL,
        retry_count INT DEFAULT 0,
        error_message NVARCHAR(MAX) NULL,
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE(),
        completed_at DATETIME2 NULL
    );
END
GO

-- Create indexes for performance
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_tracking_id' AND object_id = OBJECT_ID('message_tracking'))
BEGIN
    CREATE INDEX idx_tracking_id ON message_tracking(tracking_id);
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_status' AND object_id = OBJECT_ID('message_tracking'))
BEGIN
    CREATE INDEX idx_status ON message_tracking(status);
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_created_at' AND object_id = OBJECT_ID('message_tracking'))
BEGIN
    CREATE INDEX idx_created_at ON message_tracking(created_at);
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_priority_status' AND object_id = OBJECT_ID('message_tracking'))
BEGIN
    CREATE INDEX idx_priority_status ON message_tracking(priority, status);
END
GO

PRINT 'Database and table created successfully!';
PRINT 'Table: message_tracking';
PRINT 'Indexes: idx_tracking_id, idx_status, idx_created_at, idx_priority_status';
GO

-- Verify table creation
SELECT
    COUNT(*) as row_count,
    'message_tracking' as table_name
FROM message_tracking;
GO
