CREATE TABLE [dbo].[l_ig_it_cfg_discoup_master] (
    [l_ig_it_cfg_discoup_master_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [ent_id]                        INT          NULL,
    [discoup_id]                    INT          NULL,
    [discoup_type_code_id]          SMALLINT     NULL,
    [discoup_item_level_code_id]    SMALLINT     NULL,
    [discoup_open_code_id]          SMALLINT     NULL,
    [discoup_pct_amt_code_id]       SMALLINT     NULL,
    [profit_ctr_grp_id]             INT          NULL,
    [round_type_id]                 SMALLINT     NULL,
    [store_id]                      INT          NULL,
    [assoc_tender_id]               INT          NULL,
    [security_id]                   INT          NULL,
    [dv_load_date_time]             DATETIME     NOT NULL,
    [dv_r_load_source_id]           BIGINT       NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL,
    [dv_hash]                       CHAR (32)    NOT NULL,
    [dv_batch_id]                   BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ig_it_cfg_discoup_master]([dv_batch_id] ASC);

