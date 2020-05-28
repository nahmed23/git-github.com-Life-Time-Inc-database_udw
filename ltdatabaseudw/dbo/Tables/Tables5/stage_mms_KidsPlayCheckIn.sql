CREATE TABLE [dbo].[stage_mms_KidsPlayCheckIn] (
    [stage_mms_KidsPlayCheckIn_id] BIGINT      NOT NULL,
    [KidsPlayCheckInID]            INT         NULL,
    [ChildCenterUsageID]           INT         NULL,
    [KidsPlayCheckinDateTime]      DATETIME    NULL,
    [UTCKidsPlayCheckinDateTime]   DATETIME    NULL,
    [KidsPlayCheckinDateTimeZone]  VARCHAR (4) NULL,
    [InsertedDatetime]             DATETIME    NULL,
    [UpdatedDateTime]              DATETIME    NULL,
    [dv_batch_id]                  BIGINT      NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

