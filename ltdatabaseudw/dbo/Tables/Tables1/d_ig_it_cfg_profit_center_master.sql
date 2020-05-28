CREATE TABLE [dbo].[d_ig_it_cfg_profit_center_master] (
    [d_ig_it_cfg_profit_center_master_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)    NOT NULL,
    [dummy_bk_hash_key]                   CHAR (32)    NULL,
    [profit_center_id]                    INT          NULL,
    [store_id]                            INT          NULL,
    [p_ig_it_cfg_profit_center_master_id] BIGINT       NOT NULL,
    [dv_load_date_time]                   DATETIME     NULL,
    [dv_load_end_date_time]               DATETIME     NULL,
    [dv_batch_id]                         BIGINT       NOT NULL,
    [dv_inserted_date_time]               DATETIME     NOT NULL,
    [dv_insert_user]                      VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                DATETIME     NULL,
    [dv_update_user]                      VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_ig_it_cfg_profit_center_master]([dv_batch_id] ASC);

