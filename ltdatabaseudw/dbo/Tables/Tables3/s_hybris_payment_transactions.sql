CREATE TABLE [dbo].[s_hybris_payment_transactions] (
    [s_hybris_payment_transactions_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)       NOT NULL,
    [hjmpts]                           BIGINT          NULL,
    [payment_transactions_pk]          BIGINT          NULL,
    [created_ts]                       DATETIME        NULL,
    [modified_ts]                      DATETIME        NULL,
    [acl_ts]                           INT             NULL,
    [prop_ts]                          INT             NULL,
    [p_currency]                       BIGINT          NULL,
    [p_order]                          BIGINT          NULL,
    [p_payment_provider]               NVARCHAR (255)  NULL,
    [p_request_token]                  NVARCHAR (255)  NULL,
    [p_info]                           BIGINT          NULL,
    [p_planned_amount]                 DECIMAL (30, 8) NULL,
    [p_auth_error_code]                NVARCHAR (255)  NULL,
    [p_kount_response_code]            NVARCHAR (255)  NULL,
    [dv_load_date_time]                DATETIME        NOT NULL,
    [dv_r_load_source_id]              BIGINT          NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL,
    [dv_hash]                          CHAR (32)       NOT NULL,
    [dv_batch_id]                      BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_payment_transactions]
    ON [dbo].[s_hybris_payment_transactions]([bk_hash] ASC, [s_hybris_payment_transactions_id] ASC);

