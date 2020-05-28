CREATE TABLE [dbo].[d_exerp_master_product] (
    [d_exerp_master_product_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [master_product_id]         INT            NULL,
    [master_product_global_id]  VARCHAR (4000) NULL,
    [master_product_name]       VARCHAR (4000) NULL,
    [master_product_state]      VARCHAR (4000) NULL,
    [p_exerp_master_product_id] BIGINT         NOT NULL,
    [deleted_flag]              INT            NULL,
    [dv_load_date_time]         DATETIME       NULL,
    [dv_load_end_date_time]     DATETIME       NULL,
    [dv_batch_id]               BIGINT         NOT NULL,
    [dv_inserted_date_time]     DATETIME       NOT NULL,
    [dv_insert_user]            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]      DATETIME       NULL,
    [dv_update_user]            VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

