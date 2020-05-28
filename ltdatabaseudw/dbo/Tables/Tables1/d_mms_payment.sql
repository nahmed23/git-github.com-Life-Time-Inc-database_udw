CREATE TABLE [dbo].[d_mms_payment] (
    [d_mms_payment_id]                 BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)       NOT NULL,
    [fact_mms_payment_key]             CHAR (32)       NULL,
    [payment_id]                       INT             NULL,
    [approval_code]                    VARCHAR (50)    NULL,
    [mms_inserted_date_time]           DATETIME        NULL,
    [mms_tran_id]                      INT             NULL,
    [payment_amount]                   NUMERIC (12, 2) NULL,
    [payment_dim_date_key]             CHAR (8)        NULL,
    [payment_dim_time_key]             CHAR (8)        NULL,
    [payment_type_dim_description_key] NVARCHAR (100)  NULL,
    [tip_amount]                       NUMERIC (12, 2) NULL,
    [val_payment_type_id]              INT             NULL,
    [p_mms_payment_id]                 BIGINT          NOT NULL,
    [deleted_flag]                     INT             NULL,
    [dv_load_date_time]                DATETIME        NULL,
    [dv_load_end_date_time]            DATETIME        NULL,
    [dv_batch_id]                      BIGINT          NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

