CREATE TABLE [dbo].[stage_exerp_subscription_period] (
    [stage_exerp_subscription_period_id] BIGINT         NOT NULL,
    [id]                                 VARCHAR (4000) NULL,
    [subscription_id]                    VARCHAR (4000) NULL,
    [type]                               VARCHAR (4000) NULL,
    [state]                              VARCHAR (4000) NULL,
    [from_date]                          DATETIME       NULL,
    [to_date]                            DATETIME       NULL,
    [sale_log_id]                        VARCHAR (4000) NULL,
    [center_id]                          INT            NULL,
    [ets]                                BIGINT         NULL,
    [dummy_modified_date_time]           DATETIME       NULL,
    [dv_batch_id]                        BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

