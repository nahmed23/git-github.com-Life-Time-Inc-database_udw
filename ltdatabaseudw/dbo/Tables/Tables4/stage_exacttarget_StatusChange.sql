CREATE TABLE [dbo].[stage_exacttarget_StatusChange] (
    [stage_exacttarget_StatusChange_id] BIGINT         NOT NULL,
    [ClientID]                          BIGINT         NULL,
    [SubscriberKey]                     VARCHAR (4000) NULL,
    [EmailAddress]                      VARCHAR (4000) NULL,
    [SubscriberID]                      BIGINT         NULL,
    [OldStatus]                         VARCHAR (4000) NULL,
    [NewStatus]                         VARCHAR (4000) NULL,
    [DateChanged]                       DATETIME       NULL,
    [jan_one]                           DATETIME       NULL,
    [dv_inserted_date_time]             DATETIME       NOT NULL,
    [dv_insert_user]                    VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]              DATETIME       NULL,
    [dv_update_user]                    VARCHAR (50)   NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

