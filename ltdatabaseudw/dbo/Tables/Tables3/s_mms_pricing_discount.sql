CREATE TABLE [dbo].[s_mms_pricing_discount] (
    [s_mms_pricing_discount_id]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)       NOT NULL,
    [pricing_discount_id]             INT             NULL,
    [discount_value]                  DECIMAL (10, 2) NULL,
    [sales_commission_percent]        DECIMAL (6, 2)  NULL,
    [available_for_all_products_flag] BIT             NULL,
    [all_products_discount_use_limit] INT             NULL,
    [inserted_date_time]              DATETIME        NULL,
    [updated_date_time]               DATETIME        NULL,
    [service_commission_percent]      DECIMAL (6, 2)  NULL,
    [effective_from_date_time]        DATETIME        NULL,
    [effective_thru_date_time]        DATETIME        NULL,
    [description]                     VARCHAR (50)    NULL,
    [must_buy_all_flag]               BIT             NULL,
    [bundle_discount_flag]            BIT             NULL,
    [product_added_from_date]         DATETIME        NULL,
    [product_added_to_date]           DATETIME        NULL,
    [dv_load_date_time]               DATETIME        NOT NULL,
    [dv_batch_id]                     BIGINT          NOT NULL,
    [dv_r_load_source_id]             BIGINT          NOT NULL,
    [dv_inserted_date_time]           DATETIME        NOT NULL,
    [dv_insert_user]                  VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]            DATETIME        NULL,
    [dv_update_user]                  VARCHAR (50)    NULL,
    [dv_hash]                         CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_pricing_discount]
    ON [dbo].[s_mms_pricing_discount]([bk_hash] ASC, [s_mms_pricing_discount_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_pricing_discount]([dv_batch_id] ASC);

