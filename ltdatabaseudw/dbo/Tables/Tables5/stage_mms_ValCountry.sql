CREATE TABLE [dbo].[stage_mms_ValCountry] (
    [stage_mms_ValCountry_id] BIGINT       NOT NULL,
    [ValCountryID]            INT          NULL,
    [Description]             VARCHAR (50) NULL,
    [SortOrder]               INT          NULL,
    [InsertedDateTime]        DATETIME     NULL,
    [Abbreviation]            VARCHAR (15) NULL,
    [UpdatedDateTime]         DATETIME     NULL,
    [dv_batch_id]             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

