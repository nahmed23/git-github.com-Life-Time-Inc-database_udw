CREATE TABLE [dbo].[stage_mms_ClubPhone] (
    [stage_mms_ClubPhone_id] BIGINT      NOT NULL,
    [ClubPhoneID]            INT         NULL,
    [ClubID]                 INT         NULL,
    [AreaCode]               VARCHAR (3) NULL,
    [ValPhoneTypeID]         INT         NULL,
    [Number]                 VARCHAR (7) NULL,
    [InsertedDateTime]       DATETIME    NULL,
    [UpdatedDateTime]        DATETIME    NULL,
    [dv_batch_id]            BIGINT      NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

