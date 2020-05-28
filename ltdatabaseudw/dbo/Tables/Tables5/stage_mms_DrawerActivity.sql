CREATE TABLE [dbo].[stage_mms_DrawerActivity] (
    [stage_mms_DrawerActivity_id] BIGINT         NOT NULL,
    [DrawerActivityID]            INT            NULL,
    [DrawerID]                    INT            NULL,
    [OpenDateTime]                DATETIME       NULL,
    [CloseDateTime]               DATETIME       NULL,
    [OpenEmployeeID]              INT            NULL,
    [CloseEmployeeID]             INT            NULL,
    [ValDrawerStatusID]           INT            NULL,
    [UTCOpenDateTime]             DATETIME       NULL,
    [OpenDateTimeZone]            VARCHAR (4)    NULL,
    [UTCCloseDateTime]            DATETIME       NULL,
    [CloseDateTimeZone]           VARCHAR (4)    NULL,
    [InsertedDateTime]            DATETIME       NULL,
    [UpdatedDateTime]             DATETIME       NULL,
    [PendDateTime]                DATETIME       NULL,
    [PendEmployeeID]              INT            NULL,
    [PendDateTimeZone]            VARCHAR (4)    NULL,
    [UTCPendDateTime]             DATETIME       NULL,
    [ClosingComments]             NVARCHAR (527) NULL,
    [dv_batch_id]                 BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

