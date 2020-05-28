CREATE TABLE [dbo].[stage_mms_ResourceUsage] (
    [stage_mms_ResourceUsage_id]   BIGINT       NOT NULL,
    [ResourceUsageID]              INT          NULL,
    [LTFResourceID]                INT          NULL,
    [LTFKeyOwnerID]                INT          NULL,
    [ValResourceUsageSourceTypeID] INT          NULL,
    [PartyID]                      INT          NULL,
    [UsageDateTime]                DATETIME     NULL,
    [UsageDateTimeZone]            VARCHAR (30) NULL,
    [InsertedDateTime]             DATETIME     NULL,
    [UpdatedDateTime]              DATETIME     NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

