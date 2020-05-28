CREATE TABLE [dbo].[s_ig_it_cfg_product_class_master] (
    [s_ig_it_cfg_product_class_master_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)     NOT NULL,
    [ent_id]                              INT           NULL,
    [prod_class_id]                       INT           NULL,
    [prod_class_name]                     NVARCHAR (16) NULL,
    [item_restricted_flag]                BIT           NULL,
    [row_version]                         BINARY (8)    NULL,
    [track_id]                            BIGINT        NULL,
    [track_action]                        NVARCHAR (1)  NULL,
    [inserted_date_time]                  DATETIME      NULL,
    [updated_date_time]                   DATETIME      NULL,
    [dv_load_date_time]                   DATETIME      NOT NULL,
    [dv_r_load_source_id]                 BIGINT        NOT NULL,
    [dv_inserted_date_time]               DATETIME      NOT NULL,
    [dv_insert_user]                      VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                DATETIME      NULL,
    [dv_update_user]                      VARCHAR (50)  NULL,
    [dv_hash]                             CHAR (32)     NOT NULL,
    [dv_batch_id]                         BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_it_cfg_product_class_master]
    ON [dbo].[s_ig_it_cfg_product_class_master]([bk_hash] ASC, [s_ig_it_cfg_product_class_master_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_it_cfg_product_class_master]([dv_batch_id] ASC);

