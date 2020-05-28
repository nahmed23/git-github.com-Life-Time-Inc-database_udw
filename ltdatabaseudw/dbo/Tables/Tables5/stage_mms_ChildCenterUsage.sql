CREATE TABLE [dbo].[stage_mms_ChildCenterUsage] (
    [stage_mms_ChildCenterUsage_id] BIGINT      NOT NULL,
    [ChildCenterUsageID]            INT         NULL,
    [MemberID]                      INT         NULL,
    [ClubID]                        INT         NULL,
    [CheckInMemberID]               INT         NULL,
    [CheckInDateTime]               DATETIME    NULL,
    [CheckOutMemberID]              INT         NULL,
    [CheckOutDateTime]              DATETIME    NULL,
    [UTCCheckInDateTime]            DATETIME    NULL,
    [CheckInDateTimeZone]           VARCHAR (4) NULL,
    [UTCCheckOutDateTime]           DATETIME    NULL,
    [CheckOutDateTimeZone]          VARCHAR (4) NULL,
    [InsertedDatetime]              DATETIME    NULL,
    [UpdatedDateTime]               DATETIME    NULL,
    [dv_batch_id]                   BIGINT      NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

