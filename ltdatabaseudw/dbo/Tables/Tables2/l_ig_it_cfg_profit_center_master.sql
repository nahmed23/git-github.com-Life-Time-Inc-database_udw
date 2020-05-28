CREATE TABLE [dbo].[l_ig_it_cfg_profit_center_master] (
    [l_ig_it_cfg_profit_center_master_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)     NOT NULL,
    [profit_center_id]                    INT           NULL,
    [data_control_group_id]               INT           NULL,
    [ent_id]                              INT           NULL,
    [store_id]                            INT           NULL,
    [default_table_layout_id]             INT           NULL,
    [merchant_id]                         NVARCHAR (50) NULL,
    [primary_language_id]                 NVARCHAR (9)  NULL,
    [secondary_language_id]               NVARCHAR (9)  NULL,
    [tip_enforcement_code_id]             SMALLINT      NULL,
    [dv_load_date_time]                   DATETIME      NOT NULL,
    [dv_batch_id]                         BIGINT        NOT NULL,
    [dv_r_load_source_id]                 BIGINT        NOT NULL,
    [dv_inserted_date_time]               DATETIME      NOT NULL,
    [dv_insert_user]                      VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                DATETIME      NULL,
    [dv_update_user]                      VARCHAR (50)  NULL,
    [dv_hash]                             CHAR (32)     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_ig_it_cfg_profit_center_master]
    ON [dbo].[l_ig_it_cfg_profit_center_master]([bk_hash] ASC, [l_ig_it_cfg_profit_center_master_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ig_it_cfg_profit_center_master]([dv_batch_id] ASC);

