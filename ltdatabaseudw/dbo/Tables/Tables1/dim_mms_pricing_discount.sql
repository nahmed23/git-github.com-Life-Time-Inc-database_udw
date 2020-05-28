CREATE TABLE [dbo].[dim_mms_pricing_discount] (
    [dim_mms_pricing_discount_id]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [dim_mms_pricing_discount_key]     CHAR (32)       NULL,
    [pricing_discount_id]              INT             NULL,
    [all_products_discount_use_limit]  INT             NULL,
    [available_for_all_products_flag]  CHAR (1)        NULL,
    [discount_application_type]        VARCHAR (50)    NULL,
    [discount_combine_rule]            VARCHAR (50)    NULL,
    [discount_type]                    VARCHAR (50)    NULL,
    [discount_value]                   DECIMAL (10, 2) NULL,
    [effective_from_date_time]         DATETIME        NULL,
    [effective_from_dim_date_key]      CHAR (32)       NULL,
    [effective_thru_date_time]         DATETIME        NULL,
    [effective_thru_dim_date_key]      CHAR (32)       NULL,
    [sales_commission_percent]         DECIMAL (6, 2)  NULL,
    [sales_promotion_pos_display_text] VARCHAR (255)   NULL,
    [sales_promotion_receipt_text]     VARCHAR (255)   NULL,
    [service_commission_percent]       DECIMAL (6, 2)  NULL,
    [dv_load_date_time]                DATETIME        NULL,
    [dv_load_end_date_time]            DATETIME        NULL,
    [dv_batch_id]                      BIGINT          NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);

