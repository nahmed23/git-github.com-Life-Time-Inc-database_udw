CREATE TABLE [dbo].[d_ig_it_cfg_discoup_master] (
    [d_ig_it_cfg_discoup_master_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)       NOT NULL,
    [dim_cafe_discount_coupon_key]   CHAR (32)       NULL,
    [ent_id]                         INT             NULL,
    [discoup_id]                     INT             NULL,
    [amount]                         DECIMAL (26, 6) NULL,
    [amount_discount_flag]           CHAR (1)        NULL,
    [amount_maximum]                 DECIMAL (26, 6) NULL,
    [discount_coupon_abbreviation_1] NVARCHAR (7)    NULL,
    [discount_coupon_abbreviation_2] NVARCHAR (7)    NULL,
    [discount_coupon_name]           NVARCHAR (16)   NULL,
    [discount_coupon_type]           VARCHAR (100)   NULL,
    [discount_percent]               DECIMAL (26, 6) NULL,
    [discount_percent_maximum]       DECIMAL (26, 6) NULL,
    [percent_discount_flag]          CHAR (1)        NULL,
    [p_ig_it_cfg_discoup_master_id]  BIGINT          NOT NULL,
    [deleted_flag]                   INT             NULL,
    [dv_load_date_time]              DATETIME        NULL,
    [dv_load_end_date_time]          DATETIME        NULL,
    [dv_batch_id]                    BIGINT          NOT NULL,
    [dv_inserted_date_time]          DATETIME        NOT NULL,
    [dv_insert_user]                 VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]           DATETIME        NULL,
    [dv_update_user]                 VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_ig_it_cfg_discoup_master]([dv_batch_id] ASC);

