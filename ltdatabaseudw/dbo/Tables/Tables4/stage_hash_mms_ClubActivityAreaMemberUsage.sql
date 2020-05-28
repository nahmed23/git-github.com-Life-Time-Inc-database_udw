CREATE TABLE [dbo].[stage_hash_mms_ClubActivityAreaMemberUsage] (
    [stage_hash_mms_ClubActivityAreaMemberUsage_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)    NOT NULL,
    [ClubActivityAreaMemberUsageID]                 INT          NULL,
    [ClubID]                                        INT          NULL,
    [ValActivityAreaID]                             INT          NULL,
    [MemberID]                                      INT          NULL,
    [UsageDateTime]                                 DATETIME     NULL,
    [UTCUsageDateTime]                              DATETIME     NULL,
    [UsageDateTimeZone]                             VARCHAR (4)  NULL,
    [InsertedDateTime]                              DATETIME     NULL,
    [UpdatedDateTime]                               DATETIME     NULL,
    [dv_load_date_time]                             DATETIME     NOT NULL,
    [dv_updated_date_time]                          DATETIME     NULL,
    [dv_update_user]                                VARCHAR (50) NULL,
    [dv_batch_id]                                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

