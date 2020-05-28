CREATE TABLE [dbo].[s_ig_it_cfg_menu_item_master] (
    [s_ig_it_cfg_menu_item_master_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)       NOT NULL,
    [ent_id]                          INT             NULL,
    [menu_item_id]                    INT             NULL,
    [menu_item_name]                  NVARCHAR (50)   NULL,
    [menu_item_abbr1]                 NVARCHAR (20)   NULL,
    [menu_item_abbr2]                 NVARCHAR (20)   NULL,
    [mi_kp_label]                     NVARCHAR (16)   NULL,
    [open_modifier_flag]              BIT             NULL,
    [mi_open_price_prompt]            BIT             NULL,
    [mi_not_active_flag]              BIT             NULL,
    [mi_weight_flag]                  BIT             NULL,
    [mi_weight_tare]                  DECIMAL (26, 6) NULL,
    [mi_discountable_flag]            BIT             NULL,
    [mi_emp_discountable_flag]        BIT             NULL,
    [mi_voidable_flag]                BIT             NULL,
    [mi_print_flag]                   BIT             NULL,
    [sku_no]                          NVARCHAR (30)   NULL,
    [mi_tax_incl_flag]                BIT             NULL,
    [mi_cost_amt]                     DECIMAL (26, 6) NULL,
    [mi_receipt_label]                NVARCHAR (16)   NULL,
    [mi_price_override_flag]          BIT             NULL,
    [covers]                          SMALLINT        NULL,
    [row_version]                     BINARY (8)      NULL,
    [kds_video_label]                 NVARCHAR (20)   NULL,
    [kds_cook_time]                   INT             NULL,
    [track_id]                        BIGINT          NULL,
    [track_action]                    NVARCHAR (1)    NULL,
    [inserted_date_time]              DATETIME        NULL,
    [updated_date_time]               DATETIME        NULL,
    [dv_load_date_time]               DATETIME        NOT NULL,
    [dv_r_load_source_id]             BIGINT          NOT NULL,
    [dv_inserted_date_time]           DATETIME        NOT NULL,
    [dv_insert_user]                  VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]            DATETIME        NULL,
    [dv_update_user]                  VARCHAR (50)    NULL,
    [dv_hash]                         CHAR (32)       NOT NULL,
    [dv_batch_id]                     BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_it_cfg_menu_item_master]
    ON [dbo].[s_ig_it_cfg_menu_item_master]([bk_hash] ASC, [s_ig_it_cfg_menu_item_master_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_it_cfg_menu_item_master]([dv_batch_id] ASC);

