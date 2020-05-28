CREATE TABLE [dbo].[lkp_mms_tran_item_dim_mms_mms_tran_key] (
    [lkp_mms_tran_item_dim_mms_mms_tran_key_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [dim_mms_mms_tran_key]                      VARCHAR (32) NULL,
    [dim_mms_tran_item_key]                     VARCHAR (32) NULL,
    [dv_load_date_time]                         DATETIME     NULL,
    [dv_batch_id]                               BIGINT       NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_mms_mms_tran_key]));

