CREATE TABLE [dbo].[d_mms_sales_promotion] (
    [d_mms_sales_promotion_id]              BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)     NOT NULL,
    [dim_mms_sales_promotion_key]           CHAR (32)     NULL,
    [sales_promotion_id]                    INT           NULL,
    [effective_from_date_time]              DATETIME      NULL,
    [effective_thru_date_time]              DATETIME      NULL,
    [exclude_from_attrition_reporting_flag] CHAR (1)      NULL,
    [exclude_my_health_check_flag]          CHAR (1)      NULL,
    [sales_promotion_display_text]          VARCHAR (255) NULL,
    [sales_promotion_receipt_text]          VARCHAR (50)  NULL,
    [val_revenue_reporting_category_id]     INT           NULL,
    [val_sales_promotion_type_id]           INT           NULL,
    [val_sales_reporting_category_id]       INT           NULL,
    [p_mms_sales_promotion_id]              BIGINT        NOT NULL,
    [deleted_flag]                          INT           NULL,
    [dv_load_date_time]                     DATETIME      NULL,
    [dv_load_end_date_time]                 DATETIME      NULL,
    [dv_batch_id]                           BIGINT        NOT NULL,
    [dv_inserted_date_time]                 DATETIME      NOT NULL,
    [dv_insert_user]                        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                  DATETIME      NULL,
    [dv_update_user]                        VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

