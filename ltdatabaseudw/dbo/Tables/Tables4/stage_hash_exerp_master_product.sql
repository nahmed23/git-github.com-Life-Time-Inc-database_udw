CREATE TABLE [dbo].[stage_hash_exerp_master_product] (
    [stage_hash_exerp_master_product_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)      NOT NULL,
    [id]                                 INT            NULL,
    [name]                               VARCHAR (4000) NULL,
    [state]                              VARCHAR (4000) NULL,
    [globalid]                           VARCHAR (4000) NULL,
    [dummy_modified_date_time]           DATETIME       NULL,
    [dv_load_date_time]                  DATETIME       NOT NULL,
    [dv_updated_date_time]               DATETIME       NULL,
    [dv_update_user]                     VARCHAR (50)   NULL,
    [dv_batch_id]                        BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

