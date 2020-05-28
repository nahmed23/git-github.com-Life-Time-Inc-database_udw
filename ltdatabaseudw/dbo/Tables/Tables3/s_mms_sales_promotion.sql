CREATE TABLE [dbo].[s_mms_sales_promotion] (
    [s_mms_sales_promotion_id]              BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)     NOT NULL,
    [sales_promotion_id]                    INT           NULL,
    [effective_from_date_time]              DATETIME      NULL,
    [effective_thru_date_time]              DATETIME      NULL,
    [display_text]                          VARCHAR (255) NULL,
    [receipt_text]                          VARCHAR (50)  NULL,
    [available_for_all_sales_channels_flag] BIT           NULL,
    [available_for_all_clubs_flag]          BIT           NULL,
    [available_for_all_customers_flag]      BIT           NULL,
    [inserted_date_time]                    DATETIME      NULL,
    [updated_date_time]                     DATETIME      NULL,
    [promotion_code_usage_limit]            INT           NULL,
    [promotion_code_required_flag]          BIT           NULL,
    [promotion_code_issuer_create_limit]    INT           NULL,
    [promotion_code_overall_create_limit]   INT           NULL,
    [exclude_my_health_check_flag]          BIT           NULL,
    [exclude_from_attrition_reporting_flag] BIT           NULL,
    [dv_load_date_time]                     DATETIME      NOT NULL,
    [dv_batch_id]                           BIGINT        NOT NULL,
    [dv_r_load_source_id]                   BIGINT        NOT NULL,
    [dv_inserted_date_time]                 DATETIME      NOT NULL,
    [dv_insert_user]                        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                  DATETIME      NULL,
    [dv_update_user]                        VARCHAR (50)  NULL,
    [dv_hash]                               CHAR (32)     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_sales_promotion]
    ON [dbo].[s_mms_sales_promotion]([bk_hash] ASC, [s_mms_sales_promotion_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_sales_promotion]([dv_batch_id] ASC);

