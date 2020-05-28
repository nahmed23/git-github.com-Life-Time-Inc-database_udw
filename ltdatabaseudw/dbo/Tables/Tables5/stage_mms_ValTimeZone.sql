CREATE TABLE [dbo].[stage_mms_ValTimeZone] (
    [stage_mms_ValTimeZone_id] BIGINT       NOT NULL,
    [ValTimeZoneID]            INT          NULL,
    [Description]              VARCHAR (50) NULL,
    [SortOrder]                INT          NULL,
    [InsertedDateTime]         DATETIME     NULL,
    [Abbreviation]             VARCHAR (25) NULL,
    [UpdatedDateTime]          DATETIME     NULL,
    [DSTOffset]                INT          NULL,
    [STOffset]                 INT          NULL,
    [dv_batch_id]              BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

