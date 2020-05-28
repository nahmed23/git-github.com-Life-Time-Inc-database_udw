CREATE TABLE [dbo].[stage_exacttarget_Lists] (
    [stage_exacttarget_Lists_id] BIGINT         NOT NULL,
    [ClientID]                   BIGINT         NULL,
    [ListID]                     BIGINT         NULL,
    [Name]                       VARCHAR (4000) NULL,
    [Description]                VARCHAR (4000) NULL,
    [DateCreated]                DATETIME       NULL,
    [Status]                     VARCHAR (4000) NULL,
    [ListType]                   VARCHAR (4000) NULL,
    [jan_one]                    DATETIME       NULL,
    [dv_inserted_date_time]      DATETIME       NOT NULL,
    [dv_insert_user]             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]       DATETIME       NULL,
    [dv_update_user]             VARCHAR (50)   NULL,
    [dv_batch_id]                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

