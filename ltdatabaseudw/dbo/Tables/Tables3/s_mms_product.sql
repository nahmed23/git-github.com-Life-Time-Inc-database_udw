CREATE TABLE [dbo].[s_mms_product] (
    [s_mms_product_id]         BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)    NOT NULL,
    [product_id]               INT          NULL,
    [name]                     VARCHAR (20) NULL,
    [description]              VARCHAR (50) NULL,
    [display_ui_flag]          BIT          NULL,
    [sort_order]               INT          NULL,
    [inserted_date_time]       DATETIME     NULL,
    [turn_off_date_time]       DATETIME     NULL,
    [start_date]               DATETIME     NULL,
    [end_date]                 DATETIME     NULL,
    [gl_account_number]        VARCHAR (10) NULL,
    [gl_sub_account_number]    VARCHAR (7)  NULL,
    [complete_package_flag]    BIT          NULL,
    [allow_zero_dollar_flag]   BIT          NULL,
    [package_product_flag]     BIT          NULL,
    [sold_not_serviced_flag]   BIT          NULL,
    [updated_date_time]        DATETIME     NULL,
    [tip_allowed_flag]         BIT          NULL,
    [jr_member_dues_flag]      BIT          NULL,
    [eligible_for_hold_flag]   BIT          NULL,
    [confirm_member_data_flag] BIT          NULL,
    [medical_product_flag]     BIT          NULL,
    [bundle_product_flag]      BIT          NULL,
    [deferred_revenue_flag]    BIT          NULL,
    [price_locked_flag]        BIT          NULL,
    [assess_as_dues_flag]      BIT          NULL,
    [sku]                      VARCHAR (50) NULL,
    [dv_load_date_time]        DATETIME     NOT NULL,
    [dv_r_load_source_id]      BIGINT       NOT NULL,
    [dv_inserted_date_time]    DATETIME     NOT NULL,
    [dv_insert_user]           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]     DATETIME     NULL,
    [dv_update_user]           VARCHAR (50) NULL,
    [dv_hash]                  CHAR (32)    NOT NULL,
    [dv_batch_id]              BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_product]([dv_batch_id] ASC);

