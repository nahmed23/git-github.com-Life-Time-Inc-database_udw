CREATE TABLE [dbo].[s_ig_it_cfg_discoup_master] (
    [s_ig_it_cfg_discoup_master_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)       NOT NULL,
    [ent_id]                        INT             NULL,
    [discoup_id]                    INT             NULL,
    [discoup_name]                  NVARCHAR (16)   NULL,
    [discoup_abbr1]                 NVARCHAR (7)    NULL,
    [discoup_abbr2]                 NVARCHAR (7)    NULL,
    [discoup_percent]               DECIMAL (26, 6) NULL,
    [discoup_max_percent]           DECIMAL (26, 6) NULL,
    [discoup_amt]                   DECIMAL (26, 6) NULL,
    [discoup_max_amt]               DECIMAL (26, 6) NULL,
    [post_acct_no]                  NVARCHAR (30)   NULL,
    [prompt_extra_data_flag]        CHAR (1)        NULL,
    [threshhold_amt]                DECIMAL (26, 6) NULL,
    [round_basis]                   INT             NULL,
    [food_rev_class_flag]           CHAR (1)        NULL,
    [bev_rev_class_flag]            CHAR (1)        NULL,
    [soda_rev_class_flag]           CHAR (1)        NULL,
    [other_rev_class_flag]          CHAR (1)        NULL,
    [exclusive_flag]                CHAR (1)        NULL,
    [discount_extra_prompt_code]    TINYINT         NULL,
    [row_version]                   BINARY (8)      NULL,
    [jan_one]                       DATETIME        NULL,
    [dv_load_date_time]             DATETIME        NOT NULL,
    [dv_r_load_source_id]           BIGINT          NOT NULL,
    [dv_inserted_date_time]         DATETIME        NOT NULL,
    [dv_insert_user]                VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]          DATETIME        NULL,
    [dv_update_user]                VARCHAR (50)    NULL,
    [dv_hash]                       CHAR (32)       NOT NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_it_cfg_discoup_master]([dv_batch_id] ASC);

