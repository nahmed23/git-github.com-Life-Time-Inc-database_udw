CREATE TABLE [dbo].[stage_hash_exerp_product_privilege_usage] (
    [stage_hash_exerp_product_privilege_usage_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)      NOT NULL,
    [id]                                          INT            NULL,
    [source_type]                                 VARCHAR (4000) NULL,
    [source_id]                                   VARCHAR (4000) NULL,
    [target_type]                                 VARCHAR (4000) NULL,
    [target_id]                                   VARCHAR (4000) NULL,
    [state]                                       VARCHAR (4000) NULL,
    [campaign_code]                               VARCHAR (4000) NULL,
    [center_id]                                   INT            NULL,
    [ets]                                         BIGINT         NULL,
    [dummy_modified_date_time]                    DATETIME       NULL,
    [dv_load_date_time]                           DATETIME       NOT NULL,
    [dv_inserted_date_time]                       DATETIME       NOT NULL,
    [dv_insert_user]                              VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                        DATETIME       NULL,
    [dv_update_user]                              VARCHAR (50)   NULL,
    [dv_batch_id]                                 BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

