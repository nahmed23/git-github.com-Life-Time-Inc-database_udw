CREATE TABLE [dbo].[stage_mms_ClubActivityAreaMemberUsage] (
    [stage_mms_ClubActivityAreaMemberUsage_id] BIGINT      NOT NULL,
    [ClubActivityAreaMemberUsageID]            INT         NULL,
    [ClubID]                                   INT         NULL,
    [ValActivityAreaID]                        INT         NULL,
    [MemberID]                                 INT         NULL,
    [UsageDateTime]                            DATETIME    NULL,
    [UTCUsageDateTime]                         DATETIME    NULL,
    [UsageDateTimeZone]                        VARCHAR (4) NULL,
    [InsertedDateTime]                         DATETIME    NULL,
    [UpdatedDateTime]                          DATETIME    NULL,
    [dv_batch_id]                              BIGINT      NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

