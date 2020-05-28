CREATE TABLE [dbo].[dim_mms_club_pos_pricing_discount] (
    [dim_mms_club_pos_pricing_discount_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [dim_mms_club_pos_pricing_discount_key] CHAR (32)       NULL,
    [pricing_discount_id]                   INT             NULL,
    [all_products_discount_use_limit]       INT             NULL,
    [available_for_all_products_flag]       CHAR (1)        NULL,
    [discount_application_type]             VARCHAR (50)    NULL,
    [discount_combine_rule]                 VARCHAR (50)    NULL,
    [discount_type]                         VARCHAR (50)    NULL,
    [discount_value]                        DECIMAL (10, 2) NULL,
    [sales_commission_percent]              DECIMAL (6, 2)  NULL,
    [sales_promotion_pos_display_text]      VARCHAR (255)   NULL,
    [sales_promotion_receipt_text]          VARCHAR (255)   NULL,
    [service_commission_percent]            DECIMAL (6, 2)  NULL,
    [val_discount_application_type_id]      BIGINT          NULL,
    [val_discount_combine_rule_id]          BIGINT          NULL,
    [val_discount_type_id]                  BIGINT          NULL,
    [p_mms_pricing_discount_id]             BIGINT          NULL,
    [p_mms_sales_promotion_id]              BIGINT          NULL,
    [dv_load_date_time]                     DATETIME        NULL,
    [dv_load_end_date_time]                 DATETIME        NULL,
    [dv_batch_id]                           BIGINT          NOT NULL,
    [dv_inserted_date_time]                 DATETIME        NOT NULL,
    [dv_insert_user]                        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                  DATETIME        NULL,
    [dv_update_user]                        VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_mms_club_pos_pricing_discount_key]));

