CREATE TABLE [dbo].[d_boss_attendance] (
    [d_boss_attendance_id]     BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)    NOT NULL,
    [reservation_id]           INT          NULL,
    [cust_code]                CHAR (10)    NULL,
    [mbr_code]                 CHAR (10)    NULL,
    [attendance_date]          DATETIME     NULL,
    [attendance_dim_date_key]  CHAR (8)     NULL,
    [checked_in_flag]          CHAR (1)     NULL,
    [dim_boss_reservation_key] CHAR (32)    NULL,
    [dv_deleted_flag]          INT          NULL,
    [p_boss_attendance_id]     BIGINT       NOT NULL,
    [deleted_flag]             INT          NULL,
    [dv_load_date_time]        DATETIME     NULL,
    [dv_load_end_date_time]    DATETIME     NULL,
    [dv_batch_id]              BIGINT       NOT NULL,
    [dv_inserted_date_time]    DATETIME     NOT NULL,
    [dv_insert_user]           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]     DATETIME     NULL,
    [dv_update_user]           VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_boss_reservation_key]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_attendance]([dv_batch_id] ASC);

