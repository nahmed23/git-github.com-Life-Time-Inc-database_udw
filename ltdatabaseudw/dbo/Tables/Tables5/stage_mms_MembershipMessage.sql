CREATE TABLE [dbo].[stage_mms_MembershipMessage] (
    [stage_mms_MembershipMessage_id] BIGINT         NOT NULL,
    [MembershipMessageID]            INT            NULL,
    [MembershipID]                   INT            NULL,
    [OpenEmployeeID]                 INT            NULL,
    [CloseEmployeeID]                INT            NULL,
    [OpenDateTime]                   DATETIME       NULL,
    [CloseDateTime]                  DATETIME       NULL,
    [ValMembershipMessageTypeID]     TINYINT        NULL,
    [ValMessageStatusID]             TINYINT        NULL,
    [ReceivedDateTime]               DATETIME       NULL,
    [Comment]                        VARCHAR (2000) NULL,
    [UTCOpenDateTime]                DATETIME       NULL,
    [OpenDateTimeZone]               VARCHAR (4)    NULL,
    [UTCCloseDateTime]               DATETIME       NULL,
    [CloseDateTimeZone]              VARCHAR (4)    NULL,
    [UTCReceivedDateTime]            DATETIME       NULL,
    [ReceivedDateTimeZone]           VARCHAR (4)    NULL,
    [InsertedDateTime]               DATETIME       NULL,
    [OpenClubID]                     INT            NULL,
    [CloseClubID]                    INT            NULL,
    [UpdatedDateTime]                DATETIME       NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

