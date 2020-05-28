CREATE TABLE [dbo].[d_boss_asi_res_inst] (
    [d_boss_asi_res_inst_id]        BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [asi_res_inst_id]               INT          NULL,
    [dim_boss_reservation_key]      CHAR (32)    NULL,
    [dim_employee_key]              CHAR (32)    NULL,
    [dv_deleted_flag]               INT          NULL,
    [instructor_end_dim_date_key]   CHAR (8)     NULL,
    [instructor_start_dim_date_key] CHAR (8)     NULL,
    [instructor_type]               CHAR (1)     NULL,
    [p_boss_asi_res_inst_id]        BIGINT       NOT NULL,
    [deleted_flag]                  INT          NULL,
    [dv_load_date_time]             DATETIME     NULL,
    [dv_load_end_date_time]         DATETIME     NULL,
    [dv_batch_id]                   BIGINT       NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_asi_res_inst]([dv_batch_id] ASC);

