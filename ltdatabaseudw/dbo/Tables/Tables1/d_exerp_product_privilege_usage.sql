CREATE TABLE [dbo].[d_exerp_product_privilege_usage] (
    [d_exerp_product_privilege_usage_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)      NOT NULL,
    [product_privilege_usage_id]         INT            NULL,
    [campaign_code]                      VARCHAR (4000) NULL,
    [d_exerp_center_bk_hash]             CHAR (32)      NULL,
    [ets]                                BIGINT         NULL,
    [product_privilege_usage_state]      VARCHAR (4000) NULL,
    [source_id]                          VARCHAR (4000) NULL,
    [source_type]                        VARCHAR (4000) NULL,
    [target_id]                          VARCHAR (4000) NULL,
    [target_type]                        VARCHAR (4000) NULL,
    [p_exerp_product_privilege_usage_id] BIGINT         NOT NULL,
    [deleted_flag]                       INT            NULL,
    [dv_load_date_time]                  DATETIME       NULL,
    [dv_load_end_date_time]              DATETIME       NULL,
    [dv_batch_id]                        BIGINT         NOT NULL,
    [dv_inserted_date_time]              DATETIME       NOT NULL,
    [dv_insert_user]                     VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]               DATETIME       NULL,
    [dv_update_user]                     VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_exerp_product_privilege_usage]([dv_batch_id] ASC);

