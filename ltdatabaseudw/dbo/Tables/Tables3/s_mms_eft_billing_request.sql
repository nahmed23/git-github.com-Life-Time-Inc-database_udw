CREATE TABLE [dbo].[s_mms_eft_billing_request] (
    [s_mms_eft_billing_request_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)     NOT NULL,
    [eft_billing_request_id]       INT           NULL,
    [file_name]                    VARCHAR (50)  NULL,
    [product_price]                VARCHAR (20)  NULL,
    [quantity]                     VARCHAR (20)  NULL,
    [total_amount]                 VARCHAR (20)  NULL,
    [payment_request_reference]    VARCHAR (50)  NULL,
    [commission_employee]          VARCHAR (20)  NULL,
    [transaction_source]           VARCHAR (20)  NULL,
    [response_code]                VARCHAR (20)  NULL,
    [message]                      VARCHAR (120) NULL,
    [inserted_date_time]           DATETIME      NULL,
    [updated_date_time]            DATETIME      NULL,
    [dv_load_date_time]            DATETIME      NOT NULL,
    [dv_r_load_source_id]          BIGINT        NOT NULL,
    [dv_inserted_date_time]        DATETIME      NOT NULL,
    [dv_insert_user]               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]         DATETIME      NULL,
    [dv_update_user]               VARCHAR (50)  NULL,
    [dv_hash]                      CHAR (32)     NOT NULL,
    [dv_deleted]                   BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                  BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

