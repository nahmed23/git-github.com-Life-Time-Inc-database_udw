CREATE TABLE [dbo].[stage_hash_mms_ResourceUsage] (
    [stage_hash_mms_ResourceUsage_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)    NOT NULL,
    [ResourceUsageID]                 INT          NULL,
    [LTFResourceID]                   INT          NULL,
    [LTFKeyOwnerID]                   INT          NULL,
    [ValResourceUsageSourceTypeID]    INT          NULL,
    [PartyID]                         INT          NULL,
    [UsageDateTime]                   DATETIME     NULL,
    [UsageDateTimeZone]               VARCHAR (30) NULL,
    [InsertedDateTime]                DATETIME     NULL,
    [UpdatedDateTime]                 DATETIME     NULL,
    [dv_load_date_time]               DATETIME     NOT NULL,
    [dv_batch_id]                     BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

