CREATE TABLE [dbo].[wrk_pega_child_center_usage_activity_area] (
    [wrk_pega_child_center_usage_activity_area_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [activity_area_dim_description_key]            VARCHAR (255) NULL,
    [description]                                  VARCHAR (255) NULL,
    [fact_mms_child_center_usage_key]              VARCHAR (32)  NULL,
    [val_activity_area_id]                         BIGINT        NULL,
    [dv_load_date_time]                            DATETIME      NULL,
    [dv_load_end_date_time]                        DATETIME      NULL,
    [dv_batch_id]                                  BIGINT        NOT NULL,
    [dv_inserted_date_time]                        DATETIME      NOT NULL,
    [dv_insert_user]                               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                         DATETIME      NULL,
    [dv_update_user]                               VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

