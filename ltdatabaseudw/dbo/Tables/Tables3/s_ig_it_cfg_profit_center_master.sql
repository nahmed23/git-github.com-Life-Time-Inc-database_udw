CREATE TABLE [dbo].[s_ig_it_cfg_profit_center_master] (
    [s_ig_it_cfg_profit_center_master_id]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)       NOT NULL,
    [profit_center_id]                      INT             NULL,
    [profit_center_name]                    NVARCHAR (30)   NULL,
    [profit_ctr_abbr1]                      NVARCHAR (7)    NULL,
    [profit_ctr_abbr2]                      NVARCHAR (7)    NULL,
    [chk_hdr_line1]                         NVARCHAR (30)   NULL,
    [chk_hdr_line2]                         NVARCHAR (30)   NULL,
    [chk_hdr_line3]                         NVARCHAR (30)   NULL,
    [chk_ftr_line1]                         NVARCHAR (30)   NULL,
    [chk_ftr_line2]                         NVARCHAR (30)   NULL,
    [chk_ftr_line3]                         NVARCHAR (30)   NULL,
    [doc_lines_advance]                     SMALLINT        NULL,
    [max_doc_lines_page]                    SMALLINT        NULL,
    [min_rcpt_lines_page]                   SMALLINT        NULL,
    [sales_tippable_flag]                   CHAR (1)        NULL,
    [print_by_rev_cat_flag]                 CHAR (1)        NULL,
    [bypass_cc_agency_threshold_amount]     DECIMAL (26, 6) NULL,
    [bypass_cc_voice_auth_threshold_amount] DECIMAL (26, 6) NULL,
    [bypass_cc_printing_threshold_amount]   DECIMAL (26, 6) NULL,
    [tip_max_percent]                       SMALLINT        NULL,
    [profit_center_desc]                    NVARCHAR (50)   NULL,
    [source_property_code]                  NVARCHAR (6)    NULL,
    [pole_display_open]                     NVARCHAR (20)   NULL,
    [pole_display_closed]                   NVARCHAR (20)   NULL,
    [row_version]                           BINARY (8)      NULL,
    [track_id]                              BIGINT          NULL,
    [track_action]                          NVARCHAR (1)    NULL,
    [inserted_date_time]                    DATETIME        NULL,
    [updated_date_time]                     DATETIME        NULL,
    [dv_load_date_time]                     DATETIME        NOT NULL,
    [dv_batch_id]                           BIGINT          NOT NULL,
    [dv_r_load_source_id]                   BIGINT          NOT NULL,
    [dv_inserted_date_time]                 DATETIME        NOT NULL,
    [dv_insert_user]                        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                  DATETIME        NULL,
    [dv_update_user]                        VARCHAR (50)    NULL,
    [dv_hash]                               CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_it_cfg_profit_center_master]
    ON [dbo].[s_ig_it_cfg_profit_center_master]([bk_hash] ASC, [s_ig_it_cfg_profit_center_master_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_it_cfg_profit_center_master]([dv_batch_id] ASC);

