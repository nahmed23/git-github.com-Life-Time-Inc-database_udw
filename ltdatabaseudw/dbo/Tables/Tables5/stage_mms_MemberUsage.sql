CREATE TABLE [dbo].[stage_mms_MemberUsage] (
    [stage_mms_MemberUsage_id] BIGINT      NOT NULL,
    [MemberUsageID]            INT         NULL,
    [ClubID]                   INT         NULL,
    [MemberID]                 INT         NULL,
    [UsageDateTime]            DATETIME    NULL,
    [UTCUsageDateTime]         DATETIME    NULL,
    [UsageDateTimeZone]        VARCHAR (4) NULL,
    [InsertedDateTime]         DATETIME    NULL,
    [UpdatedDateTime]          DATETIME    NULL,
    [CheckinDelinquentFlag]    BIT         NULL,
    [DepartmentID]             INT         NULL,
    [LTFKeyOwnerID]            INT         NULL,
    [dv_batch_id]              BIGINT      NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

