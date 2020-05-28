CREATE TABLE [dbo].[d_boss_res_cancel] (
    [d_boss_res_cancel_id]     BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)    NOT NULL,
    [cancel_dim_date_key]      CHAR (8)     NULL,
    [dim_boss_reservation_key] CHAR (32)    NULL,
    [p_boss_res_cancel_id]     BIGINT       NOT NULL,
    [dv_load_date_time]        DATETIME     NULL,
    [dv_load_end_date_time]    DATETIME     NULL,
    [dv_batch_id]              BIGINT       NOT NULL,
    [dv_inserted_date_time]    DATETIME     NOT NULL,
    [dv_insert_user]           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]     DATETIME     NULL,
    [dv_update_user]           VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_res_cancel]([dv_batch_id] ASC);

