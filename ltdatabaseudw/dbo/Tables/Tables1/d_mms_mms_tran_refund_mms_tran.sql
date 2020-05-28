CREATE TABLE [dbo].[d_mms_mms_tran_refund_mms_tran] (
    [d_mms_mms_tran_refund_mms_tran_id]       BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)    NOT NULL,
    [mms_tran_refund_mms_tran_id]             INT          NULL,
    [d_mms_mms_tran_refund_bk_hash]           VARCHAR (32) NULL,
    [inserted_date_time]                      DATETIME     NULL,
    [inserted_dim_date_key]                   VARCHAR (8)  NULL,
    [inserted_dim_time_key]                   INT          NULL,
    [mms_tran_refund_id]                      INT          NULL,
    [original_fact_mms_sales_transaction_key] VARCHAR (32) NULL,
    [original_mms_tran_id]                    INT          NULL,
    [updated_date_time]                       DATETIME     NULL,
    [updated_dim_date_key]                    VARCHAR (8)  NULL,
    [updated_dim_time_key]                    INT          NULL,
    [p_mms_mms_tran_refund_mms_tran_id]       BIGINT       NOT NULL,
    [deleted_flag]                            INT          NULL,
    [dv_load_date_time]                       DATETIME     NULL,
    [dv_load_end_date_time]                   DATETIME     NULL,
    [dv_batch_id]                             BIGINT       NOT NULL,
    [dv_inserted_date_time]                   DATETIME     NOT NULL,
    [dv_insert_user]                          VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                    DATETIME     NULL,
    [dv_update_user]                          VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

