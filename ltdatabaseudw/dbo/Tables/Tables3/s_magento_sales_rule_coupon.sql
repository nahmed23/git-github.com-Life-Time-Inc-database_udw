CREATE TABLE [dbo].[s_magento_sales_rule_coupon] (
    [s_magento_sales_rule_coupon_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)     NOT NULL,
    [coupon_id]                      INT           NULL,
    [code]                           VARCHAR (255) NULL,
    [usage_limit]                    INT           NULL,
    [usage_per_customer]             INT           NULL,
    [times_used]                     INT           NULL,
    [expiration_date]                DATETIME      NULL,
    [is_primary]                     INT           NULL,
    [created_at]                     DATETIME      NULL,
    [type]                           INT           NULL,
    [generated_by_dotmailer]         INT           NULL,
    [dummy_modified_date_time]       DATETIME      NULL,
    [dv_load_date_time]              DATETIME      NOT NULL,
    [dv_r_load_source_id]            BIGINT        NOT NULL,
    [dv_inserted_date_time]          DATETIME      NOT NULL,
    [dv_insert_user]                 VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]           DATETIME      NULL,
    [dv_update_user]                 VARCHAR (50)  NULL,
    [dv_hash]                        CHAR (32)     NOT NULL,
    [dv_deleted]                     BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                    BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

