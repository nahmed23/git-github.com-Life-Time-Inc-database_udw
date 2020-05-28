CREATE TABLE [dbo].[d_mms_tran_voided] (
    [d_mms_tran_voided_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [tran_voided_id]        INT          NULL,
    [void_comment]          VARCHAR (50) NULL,
    [void_dim_date_key]     CHAR (8)     NULL,
    [void_dim_employee_key] CHAR (32)    NULL,
    [void_dim_time_key]     INT          NULL,
    [p_mms_tran_voided_id]  BIGINT       NOT NULL,
    [dv_load_date_time]     DATETIME     NULL,
    [dv_load_end_date_time] DATETIME     NULL,
    [dv_batch_id]           BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_tran_voided]([dv_batch_id] ASC);

