CREATE TABLE [dbo].[d_exerp_product_product_group] (
    [d_exerp_product_product_group_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)      NOT NULL,
    [product_id]                       VARCHAR (4000) NULL,
    [product_group_id]                 INT            NULL,
    [d_exerp_product_group_bk_hash]    CHAR (32)      NULL,
    [dim_exerp_product_key]            CHAR (32)      NULL,
    [ets]                              BIGINT         NULL,
    [p_exerp_product_product_group_id] BIGINT         NOT NULL,
    [deleted_flag]                     INT            NULL,
    [dv_load_date_time]                DATETIME       NULL,
    [dv_load_end_date_time]            DATETIME       NULL,
    [dv_batch_id]                      BIGINT         NOT NULL,
    [dv_inserted_date_time]            DATETIME       NOT NULL,
    [dv_insert_user]                   VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]             DATETIME       NULL,
    [dv_update_user]                   VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

