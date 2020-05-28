CREATE TABLE [dbo].[stage_mms_ValState] (
    [stage_mms_ValState_id] BIGINT       NOT NULL,
    [ValStateID]            INT          NULL,
    [Description]           VARCHAR (50) NULL,
    [SortOrder]             INT          NULL,
    [ValCountryID]          INT          NULL,
    [InsertedDateTime]      DATETIME     NULL,
    [Abbreviation]          VARCHAR (15) NULL,
    [UpdatedDateTime]       DATETIME     NULL,
    [dv_batch_id]           BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

