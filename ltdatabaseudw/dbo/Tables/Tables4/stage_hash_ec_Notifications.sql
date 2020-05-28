CREATE TABLE [dbo].[stage_hash_ec_Notifications] (
    [stage_hash_ec_Notifications_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)       NOT NULL,
    [NotificationId]                 INT             NULL,
    [To]                             INT             NULL,
    [From]                           INT             NULL,
    [Subject]                        NVARCHAR (4000) NULL,
    [Message]                        NVARCHAR (4000) NULL,
    [MessageType]                    INT             NULL,
    [Status]                         INT             NULL,
    [Received]                       DATETIME        NULL,
    [SourceType]                     INT             NULL,
    [SourceId]                       NVARCHAR (50)   NULL,
    [CreatedDate]                    DATETIME        NULL,
    [UpdatedDate]                    DATETIME        NULL,
    [SourceThreadId]                 NVARCHAR (50)   NULL,
    [dv_load_date_time]              DATETIME        NOT NULL,
    [dv_updated_date_time]           DATETIME        NULL,
    [dv_update_user]                 VARCHAR (50)    NULL,
    [dv_batch_id]                    BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

