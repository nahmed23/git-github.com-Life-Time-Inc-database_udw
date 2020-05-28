CREATE TABLE [dbo].[stage_exerp_product_privilege_usage] (
    [stage_exerp_product_privilege_usage_id] BIGINT         NOT NULL,
    [id]                                     INT            NULL,
    [source_type]                            VARCHAR (4000) NULL,
    [source_id]                              VARCHAR (4000) NULL,
    [target_type]                            VARCHAR (4000) NULL,
    [target_id]                              VARCHAR (4000) NULL,
    [state]                                  VARCHAR (4000) NULL,
    [campaign_code]                          VARCHAR (4000) NULL,
    [center_id]                              INT            NULL,
    [ets]                                    BIGINT         NULL,
    [dummy_modified_date_time]               DATETIME       NULL,
    [dv_batch_id]                            BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

