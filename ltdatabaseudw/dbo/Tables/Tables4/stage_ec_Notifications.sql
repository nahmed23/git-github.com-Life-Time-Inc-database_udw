CREATE TABLE [dbo].[stage_ec_Notifications] (
    [stage_ec_Notifications_id] BIGINT          NOT NULL,
    [NotificationId]            INT             NULL,
    [To]                        INT             NULL,
    [From]                      INT             NULL,
    [Subject]                   NVARCHAR (4000) NULL,
    [Message]                   NVARCHAR (4000) NULL,
    [MessageType]               INT             NULL,
    [Status]                    INT             NULL,
    [Received]                  DATETIME        NULL,
    [SourceType]                INT             NULL,
    [SourceId]                  NVARCHAR (50)   NULL,
    [CreatedDate]               DATETIME        NULL,
    [UpdatedDate]               DATETIME        NULL,
    [SourceThreadId]            NVARCHAR (50)   NULL,
    [dv_batch_id]               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

