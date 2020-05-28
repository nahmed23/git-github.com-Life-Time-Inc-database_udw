CREATE TABLE [dbo].[stage_hash_mms_MemberUsage] (
    [stage_hash_mms_MemberUsage_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [MemberUsageID]                 INT          NULL,
    [ClubID]                        INT          NULL,
    [MemberID]                      INT          NULL,
    [UsageDateTime]                 DATETIME     NULL,
    [UTCUsageDateTime]              DATETIME     NULL,
    [UsageDateTimeZone]             VARCHAR (4)  NULL,
    [InsertedDateTime]              DATETIME     NULL,
    [UpdatedDateTime]               DATETIME     NULL,
    [CheckinDelinquentFlag]         BIT          NULL,
    [DepartmentID]                  INT          NULL,
    [LTFKeyOwnerID]                 INT          NULL,
    [dv_load_date_time]             DATETIME     NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL,
    [dv_batch_id]                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

