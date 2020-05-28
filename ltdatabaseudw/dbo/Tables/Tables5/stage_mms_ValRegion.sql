CREATE TABLE [dbo].[stage_mms_ValRegion] (
    [stage_mms_ValRegion_id] BIGINT        NOT NULL,
    [ValRegionID]            INT           NULL,
    [Description]            VARCHAR (50)  NULL,
    [SortOrder]              INT           NULL,
    [CorporateIDList]        VARCHAR (255) NULL,
    [InsertedDateTime]       DATETIME      NULL,
    [UpdatedDateTime]        DATETIME      NULL,
    [dv_batch_id]            BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

