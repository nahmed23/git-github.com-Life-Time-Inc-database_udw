CREATE TABLE [dbo].[stage_exerp_subscription_state_log] (
    [stage_exerp_subscription_state_log_id] BIGINT         NOT NULL,
    [id]                                    INT            NULL,
    [center_id]                             INT            NULL,
    [subscription_id]                       VARCHAR (4000) NULL,
    [state]                                 VARCHAR (4000) NULL,
    [sub_state]                             VARCHAR (4000) NULL,
    [entry_start_datetime]                  DATETIME       NULL,
    [ets]                                   BIGINT         NULL,
    [dummy_modified_date_time]              DATETIME       NULL,
    [dv_batch_id]                           BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

