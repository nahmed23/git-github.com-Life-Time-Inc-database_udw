CREATE TABLE [dbo].[d_mms_mms_tran_refund] (
    [d_mms_mms_tran_refund_id]       BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)    NOT NULL,
    [mms_tran_refund_id]             INT          NULL,
    [fact_mms_sales_transaction_key] VARCHAR (32) NULL,
    [inserted_date_time]             DATETIME     NULL,
    [inserted_dim_date_key]          VARCHAR (8)  NULL,
    [inserted_dim_time_key]          INT          NULL,
    [mms_tran_id]                    INT          NULL,
    [requesting_club_id]             INT          NULL,
    [requesting_dim_club_key]        VARCHAR (32) NULL,
    [updated_date_time]              DATETIME     NULL,
    [updated_dim_date_key]           VARCHAR (8)  NULL,
    [updated_dim_time_key]           INT          NULL,
    [p_mms_mms_tran_refund_id]       BIGINT       NOT NULL,
    [deleted_flag]                   INT          NULL,
    [dv_load_date_time]              DATETIME     NULL,
    [dv_load_end_date_time]          DATETIME     NULL,
    [dv_batch_id]                    BIGINT       NOT NULL,
    [dv_inserted_date_time]          DATETIME     NOT NULL,
    [dv_insert_user]                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]           DATETIME     NULL,
    [dv_update_user]                 VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

